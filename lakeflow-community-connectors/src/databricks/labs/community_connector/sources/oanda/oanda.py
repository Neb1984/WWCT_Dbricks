"""OANDA Exchange Rates API connector.

Implements the ``rates`` table on top of the v1 Rates endpoint
``GET /v1/rates/{base}.{ext}`` (see ``oanda_api_doc.md``).

One API call returns the quotes of one base currency, for one UTC date, for
all requested quote currencies. The connector walks forward one day per call
and flattens the ``quotes`` map into one row per
``(base_currency, quote_currency, rate_date, data_set)``.
"""

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

import requests
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://api.corporatefxservices.com"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 4
INITIAL_BACKOFF_SECONDS = 1.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# API error code for "The rates requested have not yet been published" (HTTP 404).
NOT_YET_PUBLISHED_CODE = 56

VALID_FIELDS = {"averages", "midpoint", "highs", "lows", "all"}

DEFAULT_BASE_CURRENCY = "USD"
DEFAULT_DATA_SET = "OANDA"
DEFAULT_FIELDS = "all"
DEFAULT_DECIMAL_PLACES = 5
MAX_DECIMAL_PLACES = 15  # API maximum; matches the DecimalType scale below.
DEFAULT_INITIAL_LOOKBACK_DAYS = 30
DEFAULT_MAX_RECORDS_PER_BATCH = 1000
DEFAULT_MAX_DAYS_PER_BATCH = 31

# Quote values are returned as decimal strings (to avoid rounding); the API
# supports at most 15 decimal places.
RATE_VALUE_FIELDS = [
    "bid",
    "ask",
    "midpoint",
    "high_bid",
    "high_ask",
    "low_bid",
    "low_ask",
    "spot",
]
_RATE_DECIMAL = DecimalType(38, MAX_DECIMAL_PLACES)

RATES_SCHEMA = StructType(
    [
        StructField("base_currency", StringType(), False),
        StructField("quote_currency", StringType(), False),
        StructField("rate_date", DateType(), False),
        StructField("data_set", StringType(), False),
        StructField("quote_date", StringType(), True),
        *[StructField(name, _RATE_DECIMAL, True) for name in RATE_VALUE_FIELDS],
        StructField("decimal_places", StringType(), True),
        StructField("request_time", StringType(), True),
    ]
)

TABLE_SCHEMAS: Dict[str, StructType] = {
    "rates": RATES_SCHEMA,
}

TABLE_METADATA: Dict[str, dict] = {
    "rates": {
        "primary_keys": ["base_currency", "quote_currency", "rate_date", "data_set"],
        "cursor_field": "rate_date",
        "ingestion_type": "cdc",
    },
}


class OandaLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the OANDA Exchange Rates API."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = (options.get("api_key") or "").strip()
        if not api_key:
            raise ValueError("OANDA connector requires the 'api_key' connection option.")
        self._api_key = api_key
        self._base_url = (options.get("base_url") or DEFAULT_BASE_URL).strip().rstrip("/")
        self._session: Optional[requests.Session] = None

        # Init-time cap: only complete UTC days (up to yesterday) are read, so
        # a trigger never chases the current, still-changing day. The next
        # trigger creates a fresh instance and advances the cap.
        self._init_date = datetime.now(timezone.utc).date()
        self._cap_date = self._init_date - timedelta(days=1)

    # Keep the instance picklable (Spark ships readers to executors).
    def __getstate__(self):
        state = self.__dict__.copy()
        state["_session"] = None
        return state

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        meta = TABLE_METADATA[table_name]
        return {
            "primary_keys": list(meta["primary_keys"]),
            "cursor_field": meta["cursor_field"],
            "ingestion_type": meta["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        return self._read_rates(start_offset, table_options or {})

    # ------------------------------------------------------------------
    # rates
    # ------------------------------------------------------------------

    def _read_rates(
        self, start_offset: Optional[dict], table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        opts = _parse_rates_options(table_options, self._init_date)
        no_progress_offset = start_offset if start_offset is not None else {}

        cursor = (start_offset or {}).get("rate_date")
        if cursor:
            # Already caught up to the init-time cap: signal end of data.
            if cursor >= self._cap_date.isoformat():
                return iter([]), start_offset
            next_day = date.fromisoformat(cursor) + timedelta(days=1)
        else:
            next_day = opts["start_date"]

        records: List[dict] = []
        last_day_read: Optional[date] = None
        day = next_day
        days_read = 0
        while (
            day <= self._cap_date
            and days_read < opts["max_days_per_batch"]
            and len(records) < opts["max_records_per_batch"]
        ):
            body = self._fetch_rates_for_day(day, opts)
            if body is None:
                # Not yet published: stop without advancing past this day.
                break
            # Whole days only: never split one day's quotes across batches.
            records.extend(_flatten_rates(body, day, opts))
            last_day_read = day
            day += timedelta(days=1)
            days_read += 1

        if last_day_read is None:
            return iter([]), no_progress_offset

        return iter(records), {"rate_date": last_day_read.isoformat()}

    def _fetch_rates_for_day(self, day: date, opts: dict) -> Optional[dict]:
        """GET /v1/rates/{base}.json for one date. Returns None if not yet published."""
        url = f"{self._base_url}/v1/rates/{opts['base_currency']}.json"
        params: List[tuple] = [
            ("date", day.isoformat()),
            ("data_set", opts["data_set"]),
            ("decimal_places", str(opts["decimal_places"])),
        ]
        params.extend(("fields", f) for f in opts["fields"])
        params.extend(("quote", q) for q in opts["quote_currencies"])

        resp = self._get_with_retry(url, params)

        if resp.status_code == 404 and _error_code(resp) == NOT_YET_PUBLISHED_CODE:
            return None
        if resp.status_code != 200:
            raise RuntimeError(
                f"OANDA API request failed for {opts['base_currency']} on {day.isoformat()}: "
                f"HTTP {resp.status_code}: {resp.text[:500]}"
            )
        body = resp.json()
        if not isinstance(body, dict):
            raise RuntimeError(f"Unexpected OANDA response shape: {type(body).__name__}")
        return body

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @property
    def _http(self) -> requests.Session:
        if self._session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "Authorization": f"Bearer {self._api_key}",
                    "Accept": "application/json",
                    "User-Agent": "lakeflow-community-connectors-oanda",
                }
            )
            self._session = session
        return self._session

    def _get_with_retry(self, url: str, params: List[tuple]) -> requests.Response:
        backoff = INITIAL_BACKOFF_SECONDS
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._http.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            except (requests.ConnectionError, requests.Timeout):
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code not in RETRIABLE_STATUS_CODES or attempt == MAX_RETRIES - 1:
                return resp

            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            time.sleep(delay)
            backoff *= 2
        return resp  # pragma: no cover - loop always returns

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _error_code(resp: requests.Response) -> Optional[int]:
    try:
        body = resp.json()
    except ValueError:
        return None
    if isinstance(body, dict):
        code = body.get("code")
        return code if isinstance(code, int) else None
    return None


def _split_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    out: List[str] = []
    for part in str(value).split(","):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out


def _positive_int(table_options: dict, name: str, default: int) -> int:
    raw = table_options.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"Table option '{name}' must be an integer, got {raw!r}") from exc
    if value < 1:
        raise ValueError(f"Table option '{name}' must be >= 1, got {value}")
    return value


def _parse_rates_options(table_options: dict, init_date: date) -> dict:
    base_currency = (table_options.get("base_currency") or DEFAULT_BASE_CURRENCY).strip().upper()
    if not base_currency:
        raise ValueError("Table option 'base_currency' must not be empty")

    quote_currencies = [q.upper() for q in _split_csv(table_options.get("quote_currencies"))]

    fields = [f.lower() for f in _split_csv(table_options.get("fields") or DEFAULT_FIELDS)]
    invalid = [f for f in fields if f not in VALID_FIELDS]
    if invalid:
        raise ValueError(
            f"Invalid value(s) {invalid} for table option 'fields'. "
            f"Allowed: {sorted(VALID_FIELDS)}"
        )

    data_set = (table_options.get("data_set") or DEFAULT_DATA_SET).strip().upper()

    decimal_places = _positive_int(table_options, "decimal_places", DEFAULT_DECIMAL_PLACES)
    if decimal_places > MAX_DECIMAL_PLACES:
        raise ValueError(
            f"Table option 'decimal_places' must be between 1 and {MAX_DECIMAL_PLACES}, "
            f"got {decimal_places}"
        )

    raw_start = (table_options.get("start_date") or "").strip()
    if raw_start:
        try:
            start_date = date.fromisoformat(raw_start)
        except ValueError as exc:
            raise ValueError(
                f"Table option 'start_date' must be YYYY-MM-DD, got {raw_start!r}"
            ) from exc
    else:
        lookback = _positive_int(
            table_options, "initial_lookback_days", DEFAULT_INITIAL_LOOKBACK_DAYS
        )
        start_date = init_date - timedelta(days=lookback)

    return {
        "base_currency": base_currency,
        "quote_currencies": quote_currencies,
        "fields": fields,
        "data_set": data_set,
        "decimal_places": decimal_places,
        "start_date": start_date,
        "max_records_per_batch": _positive_int(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH
        ),
        "max_days_per_batch": _positive_int(
            table_options, "max_days_per_batch", DEFAULT_MAX_DAYS_PER_BATCH
        ),
    }


def _flatten_rates(body: dict, day: date, opts: dict) -> Iterator[dict]:
    """Turn one Rates response into one row per quote currency."""
    meta = body.get("meta") or {}
    effective = meta.get("effective_params") or {}
    base_currency = body.get("base_currency") or opts["base_currency"]
    data_set = effective.get("data_set") or opts["data_set"]
    decimal_places = effective.get("decimal_places")
    request_time = meta.get("request_time")

    quotes: Any = body.get("quotes") or {}
    if not isinstance(quotes, dict):
        return
    for quote_currency, quote in quotes.items():
        if not isinstance(quote, dict):
            continue
        record = {
            "base_currency": base_currency,
            "quote_currency": quote_currency,
            "rate_date": day.isoformat(),
            "data_set": data_set,
            "quote_date": quote.get("date"),
        }
        for name in RATE_VALUE_FIELDS:
            value = quote.get(name)
            record[name] = str(value) if value is not None else None
        record["decimal_places"] = str(decimal_places) if decimal_places is not None else None
        record["request_time"] = request_time
        yield record
