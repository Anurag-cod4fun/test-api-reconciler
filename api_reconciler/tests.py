"""
tests.py — Unit and integration tests for the reconciliation framework.

Run:
    python -m pytest tests.py -v
    python tests.py          # or directly
"""

import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# ── test subject imports ──────────────────────────────────────
from api_client import _next_offset, _build_page_params, _extract_records
from alds_adapter import MockALDSAdapter, ALDSAdapterFactory
from config import (
    AldsConfig, ApiConfig, EndpointConfig,
    ReconciliationConfig,
)
from reconciler import (
    _apply_field_mapping, _drop_ignored_fields, _normalise,
    _index_by_pk, _compare_pages, EndpointResult,
)


# ─────────────────────────────────────────────────────────────
#  Fixtures / factories
# ─────────────────────────────────────────────────────────────

def make_endpoint(**kwargs) -> EndpointConfig:
    defaults = dict(
        name="test_ep",
        api_path="/test",
        alds_table="raw.test",
        primary_key="id",
        page_limit=10,
    )
    defaults.update(kwargs)
    return EndpointConfig(**defaults)


def make_api(**kwargs) -> ApiConfig:
    defaults = dict(
        name="TestAPI",
        base_url="https://api.example.com",
        auth_token="tok",
    )
    defaults.update(kwargs)
    return ApiConfig(**defaults)


def make_alds_cfg(**kwargs) -> AldsConfig:
    defaults = dict(adapter_type="mock")
    defaults.update(kwargs)
    return AldsConfig(**defaults)


# ─────────────────────────────────────────────────────────────
#  api_client tests
# ─────────────────────────────────────────────────────────────

class TestNextOffset(unittest.TestCase):

    def test_first_page(self):
        self.assertEqual(_next_offset(0, 100), 101)

    def test_second_page(self):
        self.assertEqual(_next_offset(101, 100), 202)

    def test_small_page_limit(self):
        self.assertEqual(_next_offset(0, 10), 11)
        self.assertEqual(_next_offset(11, 10), 22)

    def test_large_page_limit(self):
        self.assertEqual(_next_offset(0, 500), 501)


class TestBuildPageParams(unittest.TestCase):

    def test_basic_params(self):
        ep = make_endpoint(page_limit=50)
        params = _build_page_params(ep, offset=0, extra_static={})
        self.assertEqual(params["pagelimit"], 50)
        self.assertEqual(params["offset"], 0)

    def test_offset_applied(self):
        ep = make_endpoint(page_limit=100)
        params = _build_page_params(ep, offset=101, extra_static={})
        self.assertEqual(params["offset"], 101)

    def test_static_api_params_merged(self):
        ep = make_endpoint(api_params={"status": "active"})
        params = _build_page_params(ep, offset=0, extra_static={})
        self.assertEqual(params["status"], "active")

    def test_extra_static_merged(self):
        ep = make_endpoint()
        params = _build_page_params(ep, offset=0, extra_static={"env": "prod"})
        self.assertEqual(params["env"], "prod")


class TestExtractRecords(unittest.TestCase):

    def test_data_key(self):
        raw = {"data": [{"id": 1}], "meta": {}}
        self.assertEqual(_extract_records(raw, "ep"), [{"id": 1}])

    def test_records_key(self):
        raw = {"records": [{"id": 2}]}
        self.assertEqual(_extract_records(raw, "ep"), [{"id": 2}])

    def test_root_list(self):
        raw = [{"id": 3}]
        self.assertEqual(_extract_records(raw, "ep"), [{"id": 3}])

    def test_empty_on_unknown_shape(self):
        raw = {"unknown_key": [{"id": 4}]}
        self.assertEqual(_extract_records(raw, "ep"), [])


# ─────────────────────────────────────────────────────────────
#  Reconciler unit tests
# ─────────────────────────────────────────────────────────────

class TestFieldMapping(unittest.TestCase):

    def test_renames_key(self):
        rec = {"prod_name": "Widget", "price": 9.99}
        mapping = {"prod_name": "name"}
        result = _apply_field_mapping(rec, mapping)
        self.assertIn("name", result)
        self.assertNotIn("prod_name", result)
        self.assertEqual(result["name"], "Widget")

    def test_unmapped_keys_preserved(self):
        rec = {"a": 1, "b": 2}
        result = _apply_field_mapping(rec, {"a": "alpha"})
        self.assertIn("b", result)

    def test_empty_mapping_passthrough(self):
        rec = {"x": 1}
        self.assertEqual(_apply_field_mapping(rec, {}), {"x": 1})


class TestDropIgnoredFields(unittest.TestCase):

    def test_drops_fields(self):
        rec = {"id": 1, "last_login": "ts", "name": "Alice"}
        result = _drop_ignored_fields(rec, ["last_login"])
        self.assertNotIn("last_login", result)
        self.assertIn("name", result)

    def test_no_ignored_passthrough(self):
        rec = {"id": 1}
        self.assertEqual(_drop_ignored_fields(rec, []), {"id": 1})


class TestIndexByPk(unittest.TestCase):

    def test_basic_indexing(self):
        records = [{"id": "a", "v": 1}, {"id": "b", "v": 2}]
        idx = _index_by_pk(records, "id")
        self.assertEqual(idx["a"]["v"], 1)
        self.assertEqual(idx["b"]["v"], 2)

    def test_missing_pk_skipped(self):
        records = [{"id": "a"}, {"no_id": True}]
        idx = _index_by_pk(records, "id")
        self.assertEqual(len(idx), 1)


class TestComparePages(unittest.TestCase):

    def _ep(self, **kw) -> EndpointConfig:
        return make_endpoint(**kw)

    def test_all_match(self):
        ep = self._ep()
        api = [{"id": f"r{i}", "val": i} for i in range(5)]
        alds = list(api)   # identical
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertEqual(result.matched, 5)
        self.assertEqual(result.field_mismatches, [])
        self.assertEqual(result.missing_in_alds, [])
        self.assertEqual(result.extra_in_alds, [])

    def test_field_mismatch_detected(self):
        ep = self._ep()
        api  = [{"id": "r1", "val": 100}]
        alds = [{"id": "r1", "val": 999}]   # tampered
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertEqual(len(result.field_mismatches), 1)
        self.assertEqual(result.field_mismatches[0].mismatches[0].field_name, "val")

    def test_missing_in_alds(self):
        ep = self._ep()
        api  = [{"id": "r1"}, {"id": "r2"}]
        alds = [{"id": "r1"}]
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertIn("r2", result.missing_in_alds)

    def test_extra_in_alds(self):
        ep = self._ep()
        api  = [{"id": "r1"}]
        alds = [{"id": "r1"}, {"id": "ghost"}]
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertIn("ghost", result.extra_in_alds)

    def test_ignored_fields_excluded(self):
        ep = self._ep(ignore_fields=["ts"])
        api  = [{"id": "r1", "name": "X", "ts": "2024-01-01"}]
        alds = [{"id": "r1", "name": "X", "ts": "DIFFERENT"}]
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertEqual(result.matched, 1)
        self.assertEqual(result.field_mismatches, [])

    def test_field_mapping_applied(self):
        ep = self._ep(field_mapping={"prod_name": "name"})
        api  = [{"id": "r1", "prod_name": "Widget"}]
        alds = [{"id": "r1", "name": "Widget"}]      # already using mapped name
        result = _compare_pages(api, alds, ep, page_number=1, offset=0)
        self.assertEqual(result.matched, 1)


# ─────────────────────────────────────────────────────────────
#  MockALDSAdapter tests
# ─────────────────────────────────────────────────────────────

class TestMockALDSAdapter(unittest.TestCase):

    def setUp(self):
        self.adapter = MockALDSAdapter(make_alds_cfg())
        self.ep = make_endpoint(page_limit=10)

    def test_seed_and_fetch(self):
        records = [{"id": f"r{i}", "v": i} for i in range(25)]
        self.adapter.seed_table(self.ep.alds_table, records)

        page1 = self.adapter.fetch_page(self.ep, offset=0, page_limit=10)
        self.assertEqual(len(page1), 10)

        page3 = self.adapter.fetch_page(self.ep, offset=20, page_limit=10)
        self.assertEqual(len(page3), 5)   # only 5 left

    def test_auto_seed(self):
        # No explicit seed — should auto-generate
        page = self.adapter.fetch_page(self.ep, offset=0, page_limit=10)
        self.assertEqual(len(page), 10)

    def test_last_page_partial(self):
        records = [{"id": f"r{i}"} for i in range(15)]
        self.adapter.seed_table(self.ep.alds_table, records)
        page2 = self.adapter.fetch_page(self.ep, offset=11, page_limit=10)
        self.assertEqual(len(page2), 4)   # records 11..14


class TestALDSAdapterFactory(unittest.TestCase):

    def test_creates_mock(self):
        cfg = make_alds_cfg(adapter_type="mock")
        adapter = ALDSAdapterFactory.create(cfg)
        self.assertIsInstance(adapter, MockALDSAdapter)

    def test_unknown_type_raises(self):
        cfg = make_alds_cfg(adapter_type="unknown_backend")
        with self.assertRaises(ValueError):
            ALDSAdapterFactory.create(cfg)

    def test_custom_registration(self):
        class MyAdapter(MockALDSAdapter):
            pass

        ALDSAdapterFactory.register("custom", MyAdapter)
        cfg = make_alds_cfg(adapter_type="custom")
        adapter = ALDSAdapterFactory.create(cfg)
        self.assertIsInstance(adapter, MyAdapter)


# ─────────────────────────────────────────────────────────────
#  Pagination sequence test
# ─────────────────────────────────────────────────────────────

class TestPaginationSequence(unittest.TestCase):
    """Verifies offset progression matches the documented contract."""

    def test_offset_sequence_100(self):
        limit = 100
        offsets = [0]
        for _ in range(4):
            offsets.append(_next_offset(offsets[-1], limit))
        self.assertEqual(offsets, [0, 101, 202, 303, 404])

    def test_offset_sequence_500(self):
        limit = 500
        offsets = [0]
        for _ in range(2):
            offsets.append(_next_offset(offsets[-1], limit))
        self.assertEqual(offsets, [0, 501, 1002])


# ─────────────────────────────────────────────────────────────
#  EndpointResult.is_clean property
# ─────────────────────────────────────────────────────────────

class TestEndpointResultIsClean(unittest.TestCase):

    def _make(self, **kw) -> EndpointResult:
        r = EndpointResult(api_name="A", endpoint_name="e", alds_table="t")
        for k, v in kw.items():
            object.__setattr__(r, k, v)
        return r

    def test_clean(self):
        self.assertTrue(self._make().is_clean)

    def test_dirty_mismatches(self):
        self.assertFalse(self._make(total_field_mismatches=1).is_clean)

    def test_dirty_missing(self):
        self.assertFalse(self._make(total_missing_in_alds=1).is_clean)

    def test_dirty_extra(self):
        self.assertFalse(self._make(total_extra_in_alds=1).is_clean)

    def test_dirty_errors(self):
        r = EndpointResult(api_name="A", endpoint_name="e", alds_table="t")
        r.errors.append("boom")
        self.assertFalse(r.is_clean)


# ─────────────────────────────────────────────────────────────
#  Runner
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.discover(start_dir=".", pattern="tests.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
