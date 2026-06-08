"""
config.py — Centralized configuration for API reconciliation framework.

All API endpoints, ALDS settings, and reconciliation parameters are
defined here. Modify this file to adapt to new APIs without touching
core logic.

CURRENT MODE: MOCK TESTING
  - base_urls point at mock_server.py localhost ports
  - auth_token is "mock-token"
    - ALDS adapter_type is "rest" for the separate mock_server.py process

SWITCHING TO PRODUCTION:
  1. Replace each base_url with the real API base URL
  2. Replace each auth_token with the real bearer token
  3. Update api_path values if your real API paths differ
  4. Set ALDS_CONFIG.adapter_type to "jdbc" or "rest"
  5. Fill in ALDS_CONFIG.connection_string / database
"""

from dataclasses import dataclass, field


# ─────────────────────────────────────────────
#  Endpoint definition
# ─────────────────────────────────────────────

@dataclass(frozen=True)
class EndpointConfig:
    name: str                           # Human-readable identifier
    api_path: str                       # Relative path appended to the API base URL
    alds_table: str                     # ALDS table/collection/index to verify against
    primary_key: str                    # Field used to match records (must be unique)
    page_limit: int = 200               # Records per page (api pagelimit param)
    api_params: dict = field(default_factory=dict)   # Extra static query params
    field_mapping: dict = field(default_factory=dict) # api_field -> alds_field remapping
    ignore_fields: list = field(default_factory=list) # Fields excluded from comparison


# ─────────────────────────────────────────────
#  API definition
# ─────────────────────────────────────────────

@dataclass(frozen=True)
class ApiConfig:
    name: str
    base_url: str
    auth_token: str
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_factor: float = 1.5
    endpoints: list[EndpointConfig] = field(default_factory=list)
    default_headers: dict = field(default_factory=dict)


# ─────────────────────────────────────────────
#  ALDS adapter configuration
# ─────────────────────────────────────────────

@dataclass(frozen=True)
class AldsConfig:
    adapter_type: str               # "mock" | "jdbc" | "rest" | "bigquery"
    connection_string: str = ""
    database: str = ""
    extra: dict = field(default_factory=dict)


# ─────────────────────────────────────────────
#  Reconciliation run configuration
# ─────────────────────────────────────────────

@dataclass(frozen=True)
class ReconciliationConfig:
    concurrency: int = 5            # Max parallel endpoint workers
    report_dir: str = "reports"     # Directory for JSON/CSV reconciliation reports
    fail_fast: bool = False         # Stop on first mismatch if True
    log_level: str = "INFO"         # DEBUG | INFO | WARNING | ERROR


# ═══════════════════════════════════════════════════════════════
#  >>>  EDIT THIS SECTION TO CONFIGURE APIS  <<<
#
#  MOCK ports (while testing with mock_server.py):
#    CustomerAPI  → http://127.0.0.1:8101
#    OrdersAPI    → http://127.0.0.1:8102
#    InventoryAPI → http://127.0.0.1:8103
#
#  API paths MUST include the version prefix (/v1/, /v2/) because
#  mock_server.py registers routes as /v1/customers, /v2/orders etc.
# ═══════════════════════════════════════════════════════════════

APIS: list[ApiConfig] = [

    # ── API 1: Customer Service ──────────────────────────────────────────
    # MOCK → http://127.0.0.1:8101
    # PROD → replace base_url e.g. "https://customer-api.yourcompany.com"
    ApiConfig(
        name="CustomerAPI",
        base_url="http://127.0.0.1:8101",
        auth_token="mock-token",            # PROD: replace with real bearer token
        timeout_seconds=30,
        max_retries=3,
        retry_backoff_factor=1.5,
        default_headers={"Accept": "application/json"},
        endpoints=[
            EndpointConfig(
                name="customers",
                api_path="/v1/customers",   # PROD: adjust if path differs
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

    # ── API 2: Orders Service ────────────────────────────────────────────
    # MOCK → http://127.0.0.1:8102
    # PROD → replace base_url e.g. "https://orders-api.yourcompany.com"
    ApiConfig(
        name="OrdersAPI",
        base_url="http://127.0.0.1:8102",
        auth_token="mock-token",
        timeout_seconds=45,
        max_retries=3,
        retry_backoff_factor=2.0,
        endpoints=[
            EndpointConfig(
                name="orders",
                api_path="/v2/orders",
                alds_table="raw.orders",
                primary_key="order_id",
                page_limit=100,
                # PROD: add api_params={"status": "all"} if your API needs it
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

    # ── API 3: Inventory Service ─────────────────────────────────────────
    # MOCK → http://127.0.0.1:8103
    # PROD → replace base_url e.g. "https://inventory-api.yourcompany.com"
    ApiConfig(
        name="InventoryAPI",
        base_url="http://127.0.0.1:8103",
        auth_token="mock-token",
        timeout_seconds=30,
        max_retries=2,
        retry_backoff_factor=1.0,
        endpoints=[
            EndpointConfig(
                name="products",
                api_path="/v1/products",
                alds_table="raw.products",
                primary_key="product_id",
                page_limit=100,
                # API returns prod_name/prod_sku → ALDS stores them as name/sku
                field_mapping={"prod_name": "name", "prod_sku": "sku"},
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

# ─────────────────────────────────────────────
#  ALDS adapter
#  "rest" → reads ALDS pages from mock_server.py over HTTP
#  "jdbc" → reads from a SQL database
# ─────────────────────────────────────────────
ALDS_CONFIG = AldsConfig(
    adapter_type="rest",            # Change to "jdbc" for SQL-backed ALDS
    connection_string="http://127.0.0.1:8104",
    database="enterprise_datalake",
)

# ─────────────────────────────────────────────
#  Reconciliation run settings
# ─────────────────────────────────────────────
RECONCILIATION_CONFIG = ReconciliationConfig(
    concurrency=5,
    report_dir="reports",
    fail_fast=False,
    log_level="INFO",
)