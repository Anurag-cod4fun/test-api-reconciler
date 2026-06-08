"""
reconciler.py — Core reconciliation engine.

For each endpoint this module:
  1. Fetches one API page at a time (via api_client.paginated_fetch).
  2. Fetches the corresponding ALDS batch with the same offset/limit.
  3. Compares every record field-by-field (respecting field_mapping and ignore_fields).
  4. Collects structured mismatches, missing records, and extra records.
  5. Returns an EndpointResult with full reconciliation statistics.
"""

import asyncio
import copy
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

try:
    from api_client import build_session, paginated_fetch, _next_offset
    from alds_adapter import BaseALDSAdapter
    from config import ApiConfig, EndpointConfig, ReconciliationConfig
except ModuleNotFoundError:
    from .api_client import build_session, paginated_fetch, _next_offset
    from .alds_adapter import BaseALDSAdapter
    from .config import ApiConfig, EndpointConfig, ReconciliationConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Result data structures
# ─────────────────────────────────────────────────────────────

@dataclass
class FieldMismatch:
    primary_key_value: Any
    field_name: str
    api_value: Any
    alds_value: Any


@dataclass
class RecordDiff:
    """All field-level mismatches for a single record."""
    primary_key_value: Any
    mismatches: list[FieldMismatch] = field(default_factory=list)


@dataclass
class PageResult:
    page_number: int
    offset: int
    api_record_count: int
    alds_record_count: int
    matched: int = 0
    field_mismatches: list[RecordDiff] = field(default_factory=list)
    missing_in_alds: list[Any] = field(default_factory=list)    # PKs in API, not in ALDS
    extra_in_alds: list[Any] = field(default_factory=list)      # PKs in ALDS, not in API
    errors: list[str] = field(default_factory=list)


@dataclass
class EndpointResult:
    api_name: str
    endpoint_name: str
    alds_table: str
    total_pages: int = 0
    total_api_records: int = 0
    total_alds_records: int = 0
    total_matched: int = 0
    total_field_mismatches: int = 0
    total_missing_in_alds: int = 0
    total_extra_in_alds: int = 0
    page_results: list[PageResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def is_clean(self) -> bool:
        return (
            self.total_field_mismatches == 0
            and self.total_missing_in_alds == 0
            and self.total_extra_in_alds == 0
            and not self.errors
        )


# ─────────────────────────────────────────────────────────────
#  Normalisation helpers
# ─────────────────────────────────────────────────────────────

def _apply_field_mapping(record: dict, mapping: dict) -> dict:
    """Renames keys according to endpoint.field_mapping."""
    if not mapping:
        return record
    return {mapping.get(k, k): v for k, v in record.items()}


def _drop_ignored_fields(record: dict, ignore_fields: list[str]) -> dict:
    if not ignore_fields:
        return record
    return {k: v for k, v in record.items() if k not in ignore_fields}


def _normalise(record: dict, endpoint: EndpointConfig) -> dict:
    """Applies mapping + field exclusion to produce a comparable snapshot."""
    r = _apply_field_mapping(copy.deepcopy(record), endpoint.field_mapping)
    r = _drop_ignored_fields(r, endpoint.ignore_fields)
    return r


def _index_by_pk(records: list[dict], pk: str) -> dict[Any, dict]:
    """Returns {pk_value: record} dict.  Warns on duplicate PKs."""
    index: dict[Any, dict] = {}
    for rec in records:
        pk_val = rec.get(pk)
        if pk_val is None:
            logger.warning("Record missing primary key '%s': %s", pk, rec)
            continue
        if pk_val in index:
            logger.warning("Duplicate primary key '%s'=%s — last record kept", pk, pk_val)
        index[pk_val] = rec
    return index


# ─────────────────────────────────────────────────────────────
#  Page-level comparison
# ─────────────────────────────────────────────────────────────

def _compare_pages(
    api_records: list[dict],
    alds_records: list[dict],
    endpoint: EndpointConfig,
    page_number: int,
    offset: int,
) -> PageResult:
    """
    Compares two lists of records (one API page, one ALDS batch).
    Returns a PageResult with all identified discrepancies.
    """
    result = PageResult(
        page_number=page_number,
        offset=offset,
        api_record_count=len(api_records),
        alds_record_count=len(alds_records),
    )

    pk = endpoint.primary_key

    # Normalise both sides
    norm_api = {pk_val: rec for pk_val, rec in
                _index_by_pk([_normalise(r, endpoint) for r in api_records], pk).items()}
    norm_alds = {pk_val: rec for pk_val, rec in
                 _index_by_pk([_normalise(r, endpoint) for r in alds_records], pk).items()}

    api_keys  = set(norm_api)
    alds_keys = set(norm_alds)

    result.missing_in_alds = sorted(api_keys - alds_keys, key=str)
    result.extra_in_alds   = sorted(alds_keys - api_keys, key=str)

    # Field-level comparison for records present on both sides
    for pk_val in api_keys & alds_keys:
        api_rec  = norm_api[pk_val]
        alds_rec = norm_alds[pk_val]

        all_fields = set(api_rec) | set(alds_rec)
        field_diffs: list[FieldMismatch] = []

        for f in sorted(all_fields):
            api_val  = api_rec.get(f)
            alds_val = alds_rec.get(f)
            if api_val != alds_val:
                field_diffs.append(FieldMismatch(
                    primary_key_value=pk_val,
                    field_name=f,
                    api_value=api_val,
                    alds_value=alds_val,
                ))

        if field_diffs:
            result.field_mismatches.append(RecordDiff(
                primary_key_value=pk_val,
                mismatches=field_diffs,
            ))
        else:
            result.matched += 1

    return result


# ─────────────────────────────────────────────────────────────
#  Endpoint-level reconciliation
# ─────────────────────────────────────────────────────────────

async def reconcile_endpoint(
    api_cfg: ApiConfig,
    endpoint: EndpointConfig,
    alds_adapter: BaseALDSAdapter,
    recon_cfg: ReconciliationConfig,
) -> EndpointResult:
    """
    Orchestrates full pagination + comparison for one endpoint.

    Flow:
        offset = 0
        loop:
            api_page  = fetch from API   (pagelimit, offset)
            alds_page = fetch from ALDS  (page_limit, offset)
            compare(api_page, alds_page)
            offset = offset + page_limit + 1
            stop when API returns < page_limit records
    """
    result = EndpointResult(
        api_name=api_cfg.name,
        endpoint_name=endpoint.name,
        alds_table=endpoint.alds_table,
    )
    t0 = time.perf_counter()
    page_number = 0
    offset = 0

    logger.info(
        "═══ Reconciling [%s › %s] ↔ ALDS[%s] ═══",
        api_cfg.name, endpoint.name, endpoint.alds_table,
    )

    async with build_session(api_cfg) as session:
        async for api_page in paginated_fetch(api_cfg, endpoint, session):
            page_number += 1

            # Fetch matching ALDS batch synchronously (wrap in executor for true async)
            alds_page = await asyncio.get_event_loop().run_in_executor(
                None,
                alds_adapter.fetch_page,
                endpoint,
                offset,
                endpoint.page_limit,
            )

            page_result = _compare_pages(
                api_records=api_page,
                alds_records=alds_page,
                endpoint=endpoint,
                page_number=page_number,
                offset=offset,
            )

            result.page_results.append(page_result)
            result.total_api_records   += page_result.api_record_count
            result.total_alds_records  += page_result.alds_record_count
            result.total_matched       += page_result.matched
            result.total_field_mismatches  += sum(
                len(rd.mismatches) for rd in page_result.field_mismatches
            )
            result.total_missing_in_alds   += len(page_result.missing_in_alds)
            result.total_extra_in_alds     += len(page_result.extra_in_alds)

            if page_result.field_mismatches or page_result.missing_in_alds or page_result.extra_in_alds:
                logger.warning(
                    "[%s › %s] Page %d — mismatches=%d missing=%d extra=%d",
                    api_cfg.name, endpoint.name, page_number,
                    len(page_result.field_mismatches),
                    len(page_result.missing_in_alds),
                    len(page_result.extra_in_alds),
                )

            if recon_cfg.fail_fast and not page_result.field_mismatches == [] == page_result.missing_in_alds == page_result.extra_in_alds:
                logger.error("fail_fast=True — aborting reconciliation for %s", endpoint.name)
                break

            offset = _next_offset(offset, endpoint.page_limit)

    result.total_pages    = page_number
    result.duration_seconds = time.perf_counter() - t0

    status = "✅ CLEAN" if result.is_clean else "❌ DISCREPANCIES FOUND"
    logger.info(
        "%s [%s › %s] pages=%d api_records=%d matched=%d field_mismatches=%d missing=%d extra=%d (%.1fs)",
        status,
        api_cfg.name, endpoint.name,
        result.total_pages, result.total_api_records,
        result.total_matched, result.total_field_mismatches,
        result.total_missing_in_alds, result.total_extra_in_alds,
        result.duration_seconds,
    )
    return result
