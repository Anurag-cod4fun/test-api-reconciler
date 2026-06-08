"""
api_client.py — Async, paginated API client with retry logic.

Handles:
  - Bearer-token auth (extend for OAuth / mTLS)
  - Configurable retry with exponential back-off
  - Offset-based pagination  (offset = previous_offset + page_limit + 1)
  - Streaming pages via async generator
"""

import asyncio
import logging
import time
from typing import AsyncGenerator

import aiohttp

try:
    from config import ApiConfig, EndpointConfig
except ModuleNotFoundError:
    from .config import ApiConfig, EndpointConfig

logger = logging.getLogger(__name__)


class APIClientError(Exception):
    """Raised when an API call fails after all retries are exhausted."""


class RateLimitError(APIClientError):
    """Raised on HTTP 429 — caller may choose to back off further."""


# ─────────────────────────────────────────────────────────────
#  Low-level HTTP helpers
# ─────────────────────────────────────────────────────────────

async def _fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    max_retries: int,
    backoff_factor: float,
    timeout: int,
) -> dict:
    """
    Performs a single GET request, retrying on transient errors.

    Retry strategy:
      - HTTP 429 / 503  → always retry (rate limit / service unavailable)
      - HTTP 5xx        → retry
      - aiohttp errors  → retry
      - HTTP 4xx (not 429) → raise immediately (client error, no point retrying)

    Back-off:  sleep = backoff_factor * (2 ** attempt)
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        if attempt:
            sleep_secs = backoff_factor * (2 ** (attempt - 1))
            logger.debug("Retry %d/%d — sleeping %.1fs", attempt, max_retries, sleep_secs)
            await asyncio.sleep(sleep_secs)

        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", backoff_factor * (2 ** attempt)))
                    logger.warning("Rate-limited on %s — waiting %ds", url, retry_after)
                    await asyncio.sleep(retry_after)
                    last_exc = RateLimitError(f"HTTP 429 on {url}")
                    continue

                if resp.status >= 500:
                    text = await resp.text()
                    logger.warning("HTTP %d from %s: %s", resp.status, url, text[:200])
                    last_exc = APIClientError(f"HTTP {resp.status}: {text[:200]}")
                    continue

                resp.raise_for_status()          # 4xx → bubble up immediately
                return await resp.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Network error on %s (attempt %d): %s", url, attempt + 1, exc)
            last_exc = exc

    raise APIClientError(
        f"Exhausted {max_retries} retries for {url}"
    ) from last_exc


# ─────────────────────────────────────────────────────────────
#  Pagination logic
# ─────────────────────────────────────────────────────────────

def _build_page_params(
    endpoint: EndpointConfig,
    offset: int,
    extra_static: dict,
) -> dict:
    """
    Constructs the query-parameter dict for one page request.

    Pagination contract:
        pagelimit = endpoint.page_limit
        offset    = 0, then incremented by (page_limit + 1) each round
                    e.g. page_limit=100 → offsets: 0, 101, 202, 303 …
    """
    params = {
        "pagelimit": endpoint.page_limit,
        "offset": offset,
        **extra_static,
        **endpoint.api_params,
    }
    return params


def _next_offset(current_offset: int, page_limit: int) -> int:
    """
    Returns the next offset value.

    Rule:  next_offset = current_offset + page_limit + 1
    This ensures a non-overlapping window:
        page 1 → offset 0,   records [0  … 99 ]
        page 2 → offset 101, records [101 … 200]
        …
    """
    return current_offset + page_limit + 1


def _extract_records(raw: dict, endpoint_name: str) -> list[dict]:
    """
    Extracts the record list from the API response payload.

    Convention (adapt to your actual API contract):
      { "data": [...] }  or  { "records": [...] }  or  the root is a list.

    Returns an empty list if the payload signals end-of-data.
    """
    if isinstance(raw, list):
        return raw

    for key in ("data", "records", "items", "results", "content"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]

    logger.warning(
        "Endpoint '%s': cannot locate record list in response keys %s",
        endpoint_name,
        list(raw.keys()),
    )
    return []


# ─────────────────────────────────────────────────────────────
#  Public async generator
# ─────────────────────────────────────────────────────────────

async def paginated_fetch(
    api_cfg: ApiConfig,
    endpoint: EndpointConfig,
    session: aiohttp.ClientSession,
) -> AsyncGenerator[list[dict], None]:
    """
    Async generator that yields one page (list[dict]) per iteration.

    Usage:
        async for page in paginated_fetch(api_cfg, endpoint, session):
            process(page)

    Stops when:
      - The API returns fewer records than page_limit  (last page)
      - The API returns an empty list
    """
    url = f"{api_cfg.base_url.rstrip('/')}/{endpoint.api_path.lstrip('/')}"
    offset = 0
    page_number = 0

    logger.info(
        "[%s › %s] Starting paginated fetch (page_limit=%d)",
        api_cfg.name,
        endpoint.name,
        endpoint.page_limit,
    )

    while True:
        params = _build_page_params(endpoint, offset, extra_static={})
        page_number += 1

        logger.debug(
            "[%s › %s] Fetching page %d — offset=%d limit=%d",
            api_cfg.name, endpoint.name, page_number, offset, endpoint.page_limit,
        )

        t0 = time.perf_counter()
        raw = await _fetch_with_retry(
            session=session,
            url=url,
            params=params,
            max_retries=api_cfg.max_retries,
            backoff_factor=api_cfg.retry_backoff_factor,
            timeout=api_cfg.timeout_seconds,
        )
        elapsed = time.perf_counter() - t0

        records = _extract_records(raw, endpoint.name)

        logger.info(
            "[%s › %s] Page %d fetched %d records in %.2fs",
            api_cfg.name, endpoint.name, page_number, len(records), elapsed,
        )

        if not records:
            logger.info(
                "[%s › %s] Empty page — pagination complete after %d pages",
                api_cfg.name, endpoint.name, page_number,
            )
            break

        yield records

        if len(records) < endpoint.page_limit:
            logger.info(
                "[%s › %s] Partial page (%d < %d) — last page reached",
                api_cfg.name, endpoint.name, len(records), endpoint.page_limit,
            )
            break

        offset = _next_offset(offset, endpoint.page_limit)


# ─────────────────────────────────────────────────────────────
#  Session factory (shared across endpoints of one API)
# ─────────────────────────────────────────────────────────────

def build_session(api_cfg: ApiConfig) -> aiohttp.ClientSession:
    """Creates an aiohttp session pre-configured with auth headers."""
    headers = {
        "Authorization": f"Bearer {api_cfg.auth_token}",
        "Content-Type": "application/json",
        **api_cfg.default_headers,
    }
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    return aiohttp.ClientSession(headers=headers, connector=connector)
