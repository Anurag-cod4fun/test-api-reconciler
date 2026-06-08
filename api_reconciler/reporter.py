"""
reporter.py — Generates JSON and CSV reconciliation reports.

Outputs:
  reports/
    reconciliation_<timestamp>/
      summary.json         — top-level pass/fail stats for every endpoint
      <api>_<endpoint>.json — full page-by-page detail per endpoint
      mismatches.csv        — flat CSV of every field-level mismatch
      missing.csv           — records found in API but absent from ALDS
      extra.csv             — records found in ALDS but absent from API
"""

import csv
import dataclasses
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from reconciler import EndpointResult, FieldMismatch, RecordDiff, PageResult
except ModuleNotFoundError:
    from .reconciler import EndpointResult, FieldMismatch, RecordDiff, PageResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  JSON serialisation helper
# ─────────────────────────────────────────────────────────────

class _EnhancedEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return super().default(obj)


def _dump(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, cls=_EnhancedEncoder, default=str)
    logger.debug("Written: %s", path)


# ─────────────────────────────────────────────────────────────
#  Report builder
# ─────────────────────────────────────────────────────────────

class ReconciliationReporter:

    def __init__(self, report_dir: str) -> None:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = Path(report_dir) / f"reconciliation_{ts}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Report directory: %s", self.run_dir)

    # ── Main entry ───────────────────────────────────────────

    def write_all(self, results: list[EndpointResult]) -> Path:
        """Writes every report file and returns the run directory."""
        self._write_summary(results)
        self._write_detail(results)
        self._write_mismatches_csv(results)
        self._write_missing_csv(results)
        self._write_extra_csv(results)
        logger.info("All reports written to %s", self.run_dir)
        return self.run_dir

    # ── Summary ──────────────────────────────────────────────

    def _write_summary(self, results: list[EndpointResult]) -> None:
        overall_clean = all(r.is_clean for r in results)
        total_api     = sum(r.total_api_records for r in results)
        total_matched = sum(r.total_matched for r in results)

        summary = {
            "status": "CLEAN" if overall_clean else "DISCREPANCIES_FOUND",
            "run_utc": datetime.now(tz=timezone.utc).isoformat(),
            "totals": {
                "endpoints": len(results),
                "api_records": total_api,
                "matched": total_matched,
                "field_mismatches": sum(r.total_field_mismatches for r in results),
                "missing_in_alds":  sum(r.total_missing_in_alds for r in results),
                "extra_in_alds":    sum(r.total_extra_in_alds for r in results),
            },
            "endpoints": [
                {
                    "api": r.api_name,
                    "endpoint": r.endpoint_name,
                    "alds_table": r.alds_table,
                    "status": "CLEAN" if r.is_clean else "DISCREPANCIES",
                    "pages": r.total_pages,
                    "api_records": r.total_api_records,
                    "alds_records": r.total_alds_records,
                    "matched": r.total_matched,
                    "field_mismatches": r.total_field_mismatches,
                    "missing_in_alds": r.total_missing_in_alds,
                    "extra_in_alds": r.total_extra_in_alds,
                    "duration_seconds": round(r.duration_seconds, 2),
                }
                for r in results
            ],
        }
        _dump(summary, self.run_dir / "summary.json")

        # Print to stdout for CI pipelines
        print("\n" + "═" * 60)
        print(f"  RECONCILIATION STATUS: {summary['status']}")
        print("═" * 60)
        for ep in summary["endpoints"]:
            icon = "✅" if ep["status"] == "CLEAN" else "❌"
            print(
                f"  {icon}  {ep['api']} › {ep['endpoint']:20s}  "
                f"matched={ep['matched']:>6}  mismatches={ep['field_mismatches']:>4}  "
                f"missing={ep['missing_in_alds']:>4}  extra={ep['extra_in_alds']:>4}"
            )
        print("═" * 60 + "\n")

    # ── Per-endpoint detail ───────────────────────────────────

    def _write_detail(self, results: list[EndpointResult]) -> None:
        for r in results:
            fname = f"{r.api_name}_{r.endpoint_name}.json".replace(" ", "_")
            _dump(dataclasses.asdict(r), self.run_dir / fname)

    # ── Flat CSV: field-level mismatches ──────────────────────

    def _write_mismatches_csv(self, results: list[EndpointResult]) -> None:
        path = self.run_dir / "mismatches.csv"
        headers = [
            "api_name", "endpoint", "alds_table",
            "page_number", "offset",
            "primary_key_value", "field_name",
            "api_value", "alds_value",
        ]
        rows: list[list[Any]] = []
        for r in results:
            for pr in r.page_results:
                for rd in pr.field_mismatches:
                    for fm in rd.mismatches:
                        rows.append([
                            r.api_name, r.endpoint_name, r.alds_table,
                            pr.page_number, pr.offset,
                            fm.primary_key_value, fm.field_name,
                            fm.api_value, fm.alds_value,
                        ])
        self._write_csv(path, headers, rows)

    # ── Flat CSV: missing in ALDS ─────────────────────────────

    def _write_missing_csv(self, results: list[EndpointResult]) -> None:
        path = self.run_dir / "missing_in_alds.csv"
        headers = [
            "api_name", "endpoint", "alds_table",
            "page_number", "offset", "primary_key_value",
        ]
        rows: list[list[Any]] = []
        for r in results:
            for pr in r.page_results:
                for pk_val in pr.missing_in_alds:
                    rows.append([
                        r.api_name, r.endpoint_name, r.alds_table,
                        pr.page_number, pr.offset, pk_val,
                    ])
        self._write_csv(path, headers, rows)

    # ── Flat CSV: extra in ALDS ───────────────────────────────

    def _write_extra_csv(self, results: list[EndpointResult]) -> None:
        path = self.run_dir / "extra_in_alds.csv"
        headers = [
            "api_name", "endpoint", "alds_table",
            "page_number", "offset", "primary_key_value",
        ]
        rows: list[list[Any]] = []
        for r in results:
            for pr in r.page_results:
                for pk_val in pr.extra_in_alds:
                    rows.append([
                        r.api_name, r.endpoint_name, r.alds_table,
                        pr.page_number, pr.offset, pk_val,
                    ])
        self._write_csv(path, headers, rows)

    # ── CSV writer ────────────────────────────────────────────

    @staticmethod
    def _write_csv(path: Path, headers: list[str], rows: list[list[Any]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(headers)
            writer.writerows(rows)
        logger.debug("Written: %s (%d rows)", path, len(rows))
