"""
alds_adapter.py — Pluggable ALDS (data lake / store) read adapter.

Architecture
────────────
  BaseALDSAdapter   — abstract interface
  MockALDSAdapter   — in-memory stub for unit tests / local dev
  JdbcALDSAdapter   — skeleton for JDBC-based stores (PostgreSQL, MySQL …)
  RestALDSAdapter   — skeleton for REST-based data stores

  ALDSAdapterFactory.create() — wires adapter_type → concrete class

To add a new backend:
  1. Subclass BaseALDSAdapter and implement fetch_page().
  2. Register it in ALDSAdapterFactory.
  3. Set adapter_type in config.py.
"""

import logging
import json
import random
from abc import ABC, abstractmethod
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from api_reconciler.config import AldsConfig, EndpointConfig
except ModuleNotFoundError:
    from config import AldsConfig, EndpointConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Abstract base
# ─────────────────────────────────────────────────────────────

class BaseALDSAdapter(ABC):
    """Defines the contract every ALDS adapter must fulfill."""

    def __init__(self, alds_cfg: AldsConfig) -> None:
        self.cfg = alds_cfg

    @abstractmethod
    def fetch_page(
        self,
        endpoint: EndpointConfig,
        offset: int,
        page_limit: int,
    ) -> list[dict]:
        """
        Returns up to `page_limit` records from `endpoint.alds_table`
        starting at `offset`.

        Pagination contract mirrors the API side:
            page_limit = endpoint.page_limit
            offset     = 0, 101, 202 … (same as API)
        """

    def close(self) -> None:
        """Optional cleanup (close DB connections, sessions, etc.)."""


# ─────────────────────────────────────────────────────────────
#  Mock adapter  (used when adapter_type == "mock")
# ─────────────────────────────────────────────────────────────

class MockALDSAdapter(BaseALDSAdapter):
    """
    In-memory mock adapter.

    Generates deterministic fake records keyed by (table, primary_key).
    Set MOCK_MISMATCH_RATE > 0 to simulate data quality issues for testing.
    """

    MOCK_TOTAL_RECORDS: int = 350   # Total records per table
    MOCK_MISMATCH_RATE: float = 0.02  # 2% of records will have injected mismatches

    def __init__(self, alds_cfg: AldsConfig) -> None:
        super().__init__(alds_cfg)
        self._store: dict[str, list[dict]] = {}   # table → sorted list of records

    # ── Seeding ──────────────────────────────────────────────

    def seed_table(self, table: str, records: list[dict]) -> None:
        """Pre-populates a table; used in tests."""
        self._store[table] = list(records)

    def _ensure_seeded(self, endpoint: EndpointConfig) -> None:
        """Auto-seeds a table with mock data on first access."""
        if endpoint.alds_table in self._store:
            return

        pk = endpoint.primary_key
        total = self.MOCK_TOTAL_RECORDS
        logger.debug("MockALDSAdapter: seeding %d records into '%s'", total, endpoint.alds_table)

        records = []
        for i in range(total):
            rec: dict[str, Any] = {
                pk: f"{endpoint.name}_{i:06d}",
                "name": f"Record {i}",
                "status": random.choice(["active", "inactive"]),
                "amount": round(random.uniform(10.0, 9999.99), 2),
                "category": random.choice(["A", "B", "C"]),
                "seq": i,
            }
            # Inject deliberate mismatches
            if random.random() < self.MOCK_MISMATCH_RATE:
                rec["_alds_tampered"] = True
                rec["name"] = f"TAMPERED_{i}"

            records.append(rec)

        self._store[endpoint.alds_table] = records

    # ── Core interface ────────────────────────────────────────

    def fetch_page(
        self,
        endpoint: EndpointConfig,
        offset: int,
        page_limit: int,
    ) -> list[dict]:
        self._ensure_seeded(endpoint)
        all_records = self._store[endpoint.alds_table]
        page = all_records[offset: offset + page_limit]

        logger.debug(
            "MockALDSAdapter.fetch_page: table=%s offset=%d limit=%d → %d rows",
            endpoint.alds_table, offset, page_limit, len(page),
        )
        return page


# ─────────────────────────────────────────────────────────────
#  JDBC adapter skeleton
# ─────────────────────────────────────────────────────────────

class JdbcALDSAdapter(BaseALDSAdapter):
    """
    Skeleton for SQL-based ALDS (PostgreSQL, MySQL, Oracle, etc.)

    Dependencies (install separately):
        pip install psycopg2-binary   # PostgreSQL
        # or sqlalchemy + appropriate driver
    """

    def __init__(self, alds_cfg: AldsConfig) -> None:
        super().__init__(alds_cfg)
        # Example using psycopg2:
        # import psycopg2
        # self._conn = psycopg2.connect(alds_cfg.connection_string)
        raise NotImplementedError(
            "JdbcALDSAdapter: install psycopg2 / sqlalchemy and implement __init__"
        )

    def fetch_page(
        self,
        endpoint: EndpointConfig,
        offset: int,
        page_limit: int,
    ) -> list[dict]:
        """
        Example SQL (PostgreSQL):

            SELECT *
            FROM {endpoint.alds_table}
            ORDER BY {endpoint.primary_key}
            LIMIT {page_limit}
            OFFSET {offset}
        """
        raise NotImplementedError

    def close(self) -> None:
        # self._conn.close()
        pass


# ─────────────────────────────────────────────────────────────
#  REST adapter skeleton
# ─────────────────────────────────────────────────────────────

class RestALDSAdapter(BaseALDSAdapter):
    """
    Skeleton for REST-based data stores (Elasticsearch, custom REST API, etc.)
    """

    def __init__(self, alds_cfg: AldsConfig) -> None:
        super().__init__(alds_cfg)
        self.base_url = alds_cfg.connection_string.rstrip("/")

    def fetch_page(
        self,
        endpoint: EndpointConfig,
        offset: int,
        page_limit: int,
    ) -> list[dict]:
        if not self.base_url:
            raise ValueError("RestALDSAdapter requires connection_string to be set")

        dataset = endpoint.alds_table
        query = urlencode({"offset": offset, "pagelimit": page_limit})
        url = f"{self.base_url}/alds/{dataset}?{query}"
        request = Request(url, headers={"Accept": "application/json"})

        try:
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"ALDS HTTP error {exc.code} for {url}") from exc
        except URLError as exc:
            raise RuntimeError(f"Unable to reach ALDS at {url}") from exc

        records = payload.get("data", [])
        if not isinstance(records, list):
            raise ValueError(f"Unexpected ALDS payload from {url}: missing list under 'data'")
        return records

    def close(self) -> None:
        # self._client.close()
        pass


# ─────────────────────────────────────────────────────────────
#  Factory
# ─────────────────────────────────────────────────────────────

class ALDSAdapterFactory:
    """Resolves adapter_type string → concrete BaseALDSAdapter instance."""

    _registry: dict[str, type[BaseALDSAdapter]] = {
        "mock": MockALDSAdapter,
        "jdbc": JdbcALDSAdapter,
        "rest": RestALDSAdapter,
    }

    @classmethod
    def register(cls, adapter_type: str, adapter_cls: type[BaseALDSAdapter]) -> None:
        """Allows external code to register custom adapters at runtime."""
        cls._registry[adapter_type] = adapter_cls

    @classmethod
    def create(cls, alds_cfg: AldsConfig) -> BaseALDSAdapter:
        adapter_cls = cls._registry.get(alds_cfg.adapter_type)
        if adapter_cls is None:
            raise ValueError(
                f"Unknown adapter_type '{alds_cfg.adapter_type}'. "
                f"Available: {list(cls._registry)}"
            )
        logger.info("ALDS adapter: %s", adapter_cls.__name__)
        return adapter_cls(alds_cfg)
