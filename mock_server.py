"""
mock_server.py — Local HTTP mock server simulating all three APIs.

Starts a real aiohttp web server on localhost so api_client.py hits
actual HTTP endpoints — no mocking of aiohttp internals needed.

What it simulates
─────────────────
  CustomerAPI  →  /customers          (350 records, page_limit=100)
                  /customers/addresses (520 records, page_limit=200)

  OrdersAPI    →  /orders             (275 records, page_limit=100)
                  /orders/items       (900 records, page_limit=500)

  InventoryAPI →  /products           (180 records, page_limit=100)
                  /warehouses         (40  records, page_limit=50 )

Run:
    python mock_server.py              # server only, Ctrl-C to stop
    python mock_server.py --alds-mismatch-rate 0.05
"""

import argparse
import asyncio
import copy
import logging
import random
import sys
from typing import Any

from aiohttp import web

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  Deterministic seed so data is reproducible across server +
#  ALDS adapter runs in the same process
# ─────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

# Port assignments per API  (must match MOCK_API_CONFIGS below)
PORTS = {
    "CustomerAPI":  8101,
    "OrdersAPI":    8102,
    "InventoryAPI": 8103,
}

ALDS_PORT = 8104
ALDS_MISMATCH_RATE = 0.0


# ─────────────────────────────────────────────────────────────
#  Data generators  (called once at startup)
# ─────────────────────────────────────────────────────────────

def _gen_customers(n: int) -> list[dict]:
    statuses = ["active", "inactive", "pending"]
    return [
        {
            "customer_id": f"CUST_{i:05d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "status": random.choice(statuses),
            "tier": random.choice(["gold", "silver", "bronze"]),
            "balance": round(random.uniform(0, 50000), 2),
            "last_login_ts": "2024-01-15T10:00:00Z",   # ignored field
            "session_token": f"tok_{i}",                # ignored field
        }
        for i in range(n)
    ]


def _gen_addresses(n: int) -> list[dict]:
    cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"]
    return [
        {
            "address_id": f"ADDR_{i:06d}",
            "customer_id": f"CUST_{i % 350:05d}",
            "city": random.choice(cities),
            "pincode": f"{random.randint(100000, 999999)}",
            "is_primary": i % 3 == 0,
        }
        for i in range(n)
    ]


def _gen_orders(n: int) -> list[dict]:
    return [
        {
            "order_id": f"ORD_{i:06d}",
            "customer_id": f"CUST_{i % 350:05d}",
            "amount": round(random.uniform(100, 25000), 2),
            "status": random.choice(["placed", "shipped", "delivered", "cancelled"]),
            "item_count": random.randint(1, 10),
            "updated_at": "2024-03-01T00:00:00Z",       # ignored field
        }
        for i in range(n)
    ]


def _gen_order_items(n: int) -> list[dict]:
    return [
        {
            "item_id": f"ITEM_{i:07d}",
            "order_id": f"ORD_{i % 275:06d}",
            "product_id": f"PROD_{random.randint(0, 179):04d}",
            "qty": random.randint(1, 20),
            "unit_price": round(random.uniform(10, 5000), 2),
        }
        for i in range(n)
    ]


def _gen_products(n: int) -> list[dict]:
    return [
        {
            "product_id": f"PROD_{i:04d}",
            "prod_name": f"Product {i}",       # will be mapped → name in ALDS
            "prod_sku": f"SKU-{i:05d}",        # will be mapped → sku  in ALDS
            "category": random.choice(["Electronics", "Clothing", "Food", "Tools"]),
            "price": round(random.uniform(5, 9999), 2),
            "stock": random.randint(0, 500),
        }
        for i in range(n)
    ]


def _gen_warehouses(n: int) -> list[dict]:
    return [
        {
            "warehouse_id": f"WH_{i:03d}",
            "location": f"City {i}",
            "capacity": random.randint(1000, 100000),
            "active": i % 5 != 0,
        }
        for i in range(n)
    ]


def _tamper_value(value: Any) -> Any:
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, float):
        return round(value + 0.01, 2)
    if isinstance(value, str):
        return f"{value}_ALDS"
    return value


def _inject_alds_mismatches(records: list[dict], primary_key: str, rate: float) -> list[dict]:
    if rate <= 0:
        return records

    tampered = copy.deepcopy(records)
    candidate_fields = None

    for record in tampered:
        if random.random() >= rate:
            continue

        if candidate_fields is None:
            candidate_fields = [field for field in record.keys() if field != primary_key]

        if not candidate_fields:
            continue

        field_name = random.choice(candidate_fields)
        record[field_name] = _tamper_value(record[field_name])

    return tampered


# ─────────────────────────────────────────────────────────────
#  Master data store  (shared between HTTP server + ALDS adapter)
# ─────────────────────────────────────────────────────────────

class DataStore:
    """
    Holds the canonical dataset for both API responses and ALDS contents.
    """

    def __init__(self):
        # ── Generate canonical records ──────────────────────
        self._api: dict[str, list[dict]] = {
            "customers":         _gen_customers(350),
            "customer_addresses":_gen_addresses(520),
            "orders":            _gen_orders(275),
            "order_items":       _gen_order_items(900),
            "products":          _gen_products(180),
            "warehouses":        _gen_warehouses(40),
        }

        # ── Deep-copy for ALDS ──────────────────────────────
        self._alds: dict[str, list[dict]] = {
            "customers": _inject_alds_mismatches(self._api["customers"], "customer_id", ALDS_MISMATCH_RATE),
            "customer_addresses": _inject_alds_mismatches(self._api["customer_addresses"], "address_id", ALDS_MISMATCH_RATE),
            "orders": _inject_alds_mismatches(self._api["orders"], "order_id", ALDS_MISMATCH_RATE),
            "order_items": _inject_alds_mismatches(self._api["order_items"], "item_id", ALDS_MISMATCH_RATE),
            "products": _inject_alds_mismatches(self._api["products"], "product_id", ALDS_MISMATCH_RATE),
            "warehouses": _inject_alds_mismatches(self._api["warehouses"], "warehouse_id", ALDS_MISMATCH_RATE),
        }

        logger.info(
            "DataStore ready — %d datasets loaded (alds_mismatch_rate=%.3f)",
            len(self._api),
            ALDS_MISMATCH_RATE,
        )

    # ── API data access ──────────────────────────────────────

    def api_page(self, dataset: str, offset: int, page_limit: int) -> list[dict]:
        rows = self._api.get(dataset, [])
        return rows[offset: offset + page_limit]

    # ── ALDS data access ─────────────────────────────────────

    def alds_page(self, dataset: str, offset: int, page_limit: int) -> list[dict]:
        rows = self._alds.get(dataset, [])
        return rows[offset: offset + page_limit]


# Lazy singleton shared across the process.
STORE: DataStore | None = None

def get_store() -> DataStore:
    global STORE
    if STORE is None:
        STORE = DataStore()
    return STORE


# ─────────────────────────────────────────────────────────────
#  aiohttp route handlers
# ─────────────────────────────────────────────────────────────

def _paginate(request: web.Request, dataset: str) -> web.Response:
    try:
        offset     = int(request.rel_url.query.get("offset", 0))
        page_limit = int(request.rel_url.query.get("pagelimit", 100))
    except ValueError:
        raise web.HTTPBadRequest(reason="offset and pagelimit must be integers")

    records = get_store().api_page(dataset, offset, page_limit)
    return web.json_response({"data": records, "offset": offset, "pagelimit": page_limit})


# CustomerAPI handlers
async def customers(req: web.Request)  -> web.Response: return _paginate(req, "customers")
async def addresses(req: web.Request)  -> web.Response: return _paginate(req, "customer_addresses")

# OrdersAPI handlers
async def orders(req: web.Request)     -> web.Response: return _paginate(req, "orders")
async def order_items(req: web.Request)-> web.Response: return _paginate(req, "order_items")

# InventoryAPI handlers
async def products(req: web.Request)   -> web.Response: return _paginate(req, "products")
async def warehouses(req: web.Request) -> web.Response: return _paginate(req, "warehouses")


def _build_app(api_name: str) -> web.Application:
    app = web.Application()
    if api_name == "CustomerAPI":
        app.router.add_get("/v1/customers",           customers)
        app.router.add_get("/v1/customers/addresses", addresses)
    elif api_name == "OrdersAPI":
        app.router.add_get("/v2/orders",              orders)
        app.router.add_get("/v2/orders/items",        order_items)
    elif api_name == "InventoryAPI":
        app.router.add_get("/v1/products",            products)
        app.router.add_get("/v1/warehouses",          warehouses)
    return app


# ─────────────────────────────────────────────────────────────
#  Server runner
# ─────────────────────────────────────────────────────────────

async def start_servers() -> list[web.AppRunner]:
    """Starts all three API servers and returns their runners."""
    runners = []
    for api_name, port in PORTS.items():
        app = _build_app(api_name)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", port)
        await site.start()
        logger.info("%-14s listening on http://127.0.0.1:%d", api_name, port)
        runners.append(runner)

    alds_runner = web.AppRunner(_build_alds_app())
    await alds_runner.setup()
    alds_site = web.TCPSite(alds_runner, "127.0.0.1", ALDS_PORT)
    await alds_site.start()
    logger.info("%-14s listening on http://127.0.0.1:%d", "ALDS", ALDS_PORT)
    runners.append(alds_runner)
    return runners


async def stop_servers(runners: list[web.AppRunner]) -> None:
    for r in runners:
        await r.cleanup()


def _alds_table_to_dataset(table: str) -> str:
    return {
        "raw.customers": "customers",
        "raw.customer_addresses": "customer_addresses",
        "raw.orders": "orders",
        "raw.order_items": "order_items",
        "raw.products": "products",
        "raw.warehouses": "warehouses",
    }.get(table, table)


def _alds_paginate(request: web.Request, dataset: str) -> web.Response:
    try:
        offset = int(request.rel_url.query.get("offset", 0))
        page_limit = int(request.rel_url.query.get("pagelimit", 100))
    except ValueError:
        raise web.HTTPBadRequest(reason="offset and pagelimit must be integers")

    records = get_store().alds_page(dataset, offset, page_limit)
    return web.json_response({"data": records, "offset": offset, "pagelimit": page_limit})


async def alds_records(req: web.Request) -> web.Response:
    dataset = req.match_info["dataset"]
    return _alds_paginate(req, _alds_table_to_dataset(dataset))


def _build_alds_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/alds/{dataset}", alds_records)
    return app


# ─────────────────────────────────────────────────────────────
#  Mock ALDS adapter  (pulls from STORE._alds)
# ─────────────────────────────────────────────────────────────

# We patch the existing MockALDSAdapter to read from STORE instead of
# auto-generating data so both sides share the same seeded dataset.

from api_reconciler.alds_adapter import MockALDSAdapter
from api_reconciler.config import EndpointConfig as EC

# Map alds_table names → STORE dataset keys
_TABLE_TO_DATASET = {
    "raw.customers":          "customers",
    "raw.customer_addresses": "customer_addresses",
    "raw.orders":             "orders",
    "raw.order_items":        "order_items",
    "raw.products":           "products",
    "raw.warehouses":         "warehouses",
}


class LiveMockALDSAdapter(MockALDSAdapter):
    """
    Overrides MockALDSAdapter.fetch_page to read from STORE._alds
    instead of auto-generating records. This is the mismatch-injected copy.
    """

    def fetch_page(self, endpoint: EC, offset: int, page_limit: int) -> list[dict]:
        dataset = _TABLE_TO_DATASET.get(endpoint.alds_table, endpoint.alds_table)
        page = get_store().alds_page(dataset, offset, page_limit)
        logger.debug(
            "LiveMockALDSAdapter: table=%s offset=%d limit=%d → %d rows",
            endpoint.alds_table, offset, page_limit, len(page),
        )
        return page


# ─────────────────────────────────────────────────────────────
#  Mock config  (points at localhost servers)
# ─────────────────────────────────────────────────────────────

from api_reconciler.config import ApiConfig, EndpointConfig, AldsConfig, ReconciliationConfig

MOCK_API_CONFIGS = [
    ApiConfig(
        name="CustomerAPI",
        base_url=f"http://127.0.0.1:{PORTS['CustomerAPI']}",
        auth_token="mock-token",
        max_retries=1,
        endpoints=[
            EndpointConfig(
                name="customers",
                api_path="/v1/customers",
                alds_table="raw.customers",
                primary_key="customer_id",
                page_limit=100,
                ignore_fields=["last_login_ts", "session_token"],
            ),
            EndpointConfig(
                name="customer_addresses",
                api_path="/v1/customers/addresses",
                alds_table="raw.customer_addresses",
                primary_key="address_id",
                page_limit=200,
            ),
        ],
    ),
    ApiConfig(
        name="OrdersAPI",
        base_url=f"http://127.0.0.1:{PORTS['OrdersAPI']}",
        auth_token="mock-token",
        max_retries=1,
        endpoints=[
            EndpointConfig(
                name="orders",
                api_path="/v2/orders",
                alds_table="raw.orders",
                primary_key="order_id",
                page_limit=100,
                ignore_fields=["updated_at"],
            ),
            EndpointConfig(
                name="order_items",
                api_path="/v2/orders/items",
                alds_table="raw.order_items",
                primary_key="item_id",
                page_limit=500,
            ),
        ],
    ),
    ApiConfig(
        name="InventoryAPI",
        base_url=f"http://127.0.0.1:{PORTS['InventoryAPI']}",
        auth_token="mock-token",
        max_retries=1,
        endpoints=[
            EndpointConfig(
                name="products",
                api_path="/v1/products",
                alds_table="raw.products",
                primary_key="product_id",
                field_mapping={"prod_name": "name", "prod_sku": "sku"},
                page_limit=100,
            ),
            EndpointConfig(
                name="warehouses",
                api_path="/v1/warehouses",
                alds_table="raw.warehouses",
                primary_key="warehouse_id",
                page_limit=50,
            ),
        ],
    ),
]

MOCK_ALDS_CONFIG  = AldsConfig(adapter_type="mock")
MOCK_RECON_CONFIG = ReconciliationConfig(concurrency=6, report_dir="reports", log_level="INFO")


def _parse_args():
    p = argparse.ArgumentParser(description="Mock API server for reconciler testing")
    p.add_argument(
        "--alds-mismatch-rate",
        type=float,
        default=0.0,
        help="Inject field mismatches into ALDS data at the given rate (0.0-1.0)",
    )
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


async def _serve_forever():
    runners = await start_servers()
    print("\n  Mock API servers and ALDS server are running.")
    print("  Press Ctrl-C to stop.\n")
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await stop_servers(runners)


def main() -> int:
    args = _parse_args()
    global ALDS_MISMATCH_RATE
    ALDS_MISMATCH_RATE = max(0.0, min(1.0, args.alds_mismatch_rate))
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    asyncio.run(_serve_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
