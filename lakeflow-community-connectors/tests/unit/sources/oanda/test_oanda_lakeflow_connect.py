from datetime import date, timedelta

import pytest

from databricks.labs.community_connector.sources.oanda.oanda import (
    OandaLakeflowConnect,
)
from tests.unit.sources.test_suite import (
    MODE_SIMULATE,
    LakeflowConnectTests,
    _resolve_env_mode_for_simulator,
)


class TestOandaConnector(LakeflowConnectTests):
    connector_class = OandaLakeflowConnect
    simulator_source = "oanda"
    sample_records = 50
    # Stand-in credentials: the simulator never validates them.
    replay_config = {
        "api_key": "simulator-fake-api-key",
    }
    # ``spot`` is only returned for Central Bank data sets; the default
    # ``OANDA`` data set used by the generic suite returns bid/ask/midpoint/
    # highs/lows instead. ``test_central_bank_data_set_returns_spot`` below
    # covers the column explicitly.
    allow_null_columns = {"rates": {"spot"}}

    # ------------------------------------------------------------------
    # Connector-specific behaviour (simulate mode only: these rely on the
    # deterministic simulator corpus and would spend live quotes).
    # ------------------------------------------------------------------

    @staticmethod
    def _require_simulate():
        if _resolve_env_mode_for_simulator("oanda") != MODE_SIMULATE:
            pytest.skip("simulate-mode only")

    def test_one_row_per_quote_currency_per_day(self):
        self._require_simulate()
        start = (date.today() - timedelta(days=5)).isoformat()
        records, offset = self.connector.read_table(
            "rates", {}, {"start_date": start, "base_currency": "usd"}
        )
        rows = list(records)
        assert rows, "expected quotes for the requested window"
        assert {r["base_currency"] for r in rows} == {"USD"}
        assert "USD" not in {r["quote_currency"] for r in rows}
        keys = [(r["base_currency"], r["quote_currency"], r["rate_date"], r["data_set"]) for r in rows]
        assert len(keys) == len(set(keys)), "duplicate primary keys"
        assert min(r["rate_date"] for r in rows) == start
        assert offset["rate_date"] == max(r["rate_date"] for r in rows)

    def test_quote_currencies_filter(self):
        self._require_simulate()
        records, _ = self.connector.read_table(
            "rates", {}, {"quote_currencies": "eur, gbp", "initial_lookback_days": "2"}
        )
        rows = list(records)
        assert rows
        assert {r["quote_currency"] for r in rows} <= {"EUR", "GBP"}

    def test_central_bank_data_set_returns_spot(self):
        self._require_simulate()
        records, _ = self.connector.read_table(
            "rates", {}, {"data_set": "EUCB", "initial_lookback_days": "2"}
        )
        rows = list(records)
        assert rows
        assert all(r["data_set"] == "EUCB" for r in rows)
        assert all(r["spot"] is not None for r in rows)
        assert all(r["bid"] is None for r in rows)

    def test_batches_respect_whole_days_and_resume(self):
        self._require_simulate()
        opts = {"initial_lookback_days": "6", "max_days_per_batch": "2"}
        records, offset1 = self.connector.read_table("rates", {}, opts)
        days1 = {r["rate_date"] for r in records}
        assert len(days1) == 2
        records, offset2 = self.connector.read_table("rates", offset1, opts)
        days2 = {r["rate_date"] for r in records}
        assert len(days2) == 2
        assert min(days2) > max(days1), "resume must start after the checkpointed day"
        assert offset2["rate_date"] > offset1["rate_date"]

    def test_never_reads_current_utc_day(self):
        self._require_simulate()
        records, offset = self.connector.read_table("rates", {}, {"initial_lookback_days": "3"})
        rows = list(records)
        cap = self.connector._cap_date.isoformat()  # pylint: disable=protected-access
        assert max(r["rate_date"] for r in rows) <= cap
        # Re-reading from the cap returns the same offset (termination).
        records, again = self.connector.read_table("rates", offset, {"initial_lookback_days": "3"})
        assert list(records) == []
        assert again == offset

    def test_invalid_options_raise(self):
        with pytest.raises(ValueError):
            self.connector.read_table("rates", {}, {"fields": "bogus"})
        with pytest.raises(ValueError):
            self.connector.read_table("rates", {}, {"decimal_places": "16"})
        with pytest.raises(ValueError):
            self.connector.read_table("rates", {}, {"start_date": "10/01/2026"})

    def test_missing_api_key_raises(self):
        with pytest.raises(ValueError):
            OandaLakeflowConnect({})
