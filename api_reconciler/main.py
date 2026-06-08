import sys, os
# Ensure the directory containing this file is always on sys.path,
# regardless of CWD or how the script is invoked (python main.py,
# python -m, IDE run configs, etc.)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

"""
main.py — Entry point for the API ↔ ALDS reconciliation framework.

Orchestration:
  - All three APIs run concurrently.
  - Within each API, endpoints run concurrently up to `concurrency` limit.
  - A shared asyncio.Semaphore caps total parallel API sessions.
  - On completion, all EndpointResults are handed to ReconciliationReporter.
  - Exit code 0 = clean, 1 = discrepancies found, 2 = run error.

Usage:
    python main.py
    python main.py --fail-fast
    python main.py --log-level DEBUG
    python main.py --api CustomerAPI          # single API
    python main.py --api CustomerAPI --ep customers  # single endpoint
"""

import argparse
import asyncio
import logging
import sys
from typing import Optional

from alds_adapter import ALDSAdapterFactory
from config import APIS, ALDS_CONFIG, RECONCILIATION_CONFIG, ApiConfig, EndpointConfig
from reconciler import EndpointResult, reconcile_endpoint
from reporter import ReconciliationReporter


# ─────────────────────────────────────────────────────────────
#  Logging setup
# ─────────────────────────────────────────────────────────────

def _configure_logging(level: str) -> None:
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("reconciliation.log", encoding="utf-8"),
        ],
    )
    # Suppress noisy third-party loggers
    for noisy in ("aiohttp", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Semaphore-guarded endpoint task
# ─────────────────────────────────────────────────────────────

async def _run_endpoint_task(
    sem: asyncio.Semaphore,
    api_cfg: ApiConfig,
    endpoint: EndpointConfig,
    alds_adapter,
    recon_cfg,
) -> EndpointResult:
    async with sem:
        try:
            return await reconcile_endpoint(api_cfg, endpoint, alds_adapter, recon_cfg)
        except Exception as exc:
            logger.exception(
                "Unhandled error reconciling [%s › %s]: %s",
                api_cfg.name, endpoint.name, exc,
            )
            result = EndpointResult(
                api_name=api_cfg.name,
                endpoint_name=endpoint.name,
                alds_table=endpoint.alds_table,
            )
            result.errors.append(str(exc))
            return result


# ─────────────────────────────────────────────────────────────
#  Main async orchestrator
# ─────────────────────────────────────────────────────────────

async def run_reconciliation(
    filter_api: Optional[str] = None,
    filter_endpoint: Optional[str] = None,
) -> list[EndpointResult]:
    """
    Launches all endpoint reconciliation tasks concurrently.

    Args:
        filter_api:      If set, only processes the named API.
        filter_endpoint: If set (and filter_api is set), only that endpoint.

    Returns:
        List of EndpointResult — one per endpoint processed.
    """
    recon_cfg = RECONCILIATION_CONFIG
    alds_adapter = ALDSAdapterFactory.create(ALDS_CONFIG)
    sem = asyncio.Semaphore(recon_cfg.concurrency)

    tasks: list[asyncio.Task] = []

    for api_cfg in APIS:
        if filter_api and api_cfg.name != filter_api:
            continue

        for endpoint in api_cfg.endpoints:
            if filter_endpoint and endpoint.name != filter_endpoint:
                continue

            task = asyncio.create_task(
                _run_endpoint_task(sem, api_cfg, endpoint, alds_adapter, recon_cfg),
                name=f"{api_cfg.name}:{endpoint.name}",
            )
            tasks.append(task)

    if not tasks:
        logger.warning("No endpoints matched the filter criteria — nothing to reconcile.")
        return []

    logger.info("Spawned %d endpoint task(s) (concurrency cap=%d)", len(tasks), recon_cfg.concurrency)
    results: list[EndpointResult] = await asyncio.gather(*tasks)

    alds_adapter.close()
    return list(results)


# ─────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="API ↔ ALDS reconciliation framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --log-level DEBUG
  python main.py --api CustomerAPI
  python main.py --api OrdersAPI --ep orders
  python main.py --fail-fast
        """,
    )
    parser.add_argument("--api", metavar="API_NAME", help="Limit run to one API (by name)")
    parser.add_argument("--ep",  metavar="ENDPOINT",  help="Limit run to one endpoint (requires --api)")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first mismatch per endpoint")
    parser.add_argument("--log-level", default=RECONCILIATION_CONFIG.log_level,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────

def main() -> int:
    args = _parse_args()

    # Apply CLI overrides
    if args.fail_fast:
        object.__setattr__(RECONCILIATION_CONFIG, "fail_fast", True)

    _configure_logging(args.log_level)

    logger.info("Starting reconciliation run")
    logger.info("APIs configured : %d", len(APIS))
    logger.info("ALDS adapter    : %s", ALDS_CONFIG.adapter_type)
    logger.info("Concurrency     : %d", RECONCILIATION_CONFIG.concurrency)

    results = asyncio.run(
        run_reconciliation(
            filter_api=args.api,
            filter_endpoint=args.ep,
        )
    )

    if not results:
        logger.warning("No results — check filters.")
        return 2

    reporter = ReconciliationReporter(report_dir=RECONCILIATION_CONFIG.report_dir)
    reporter.write_all(results)

    overall_clean = all(r.is_clean for r in results)
    return 0 if overall_clean else 1


if __name__ == "__main__":
    sys.exit(main())