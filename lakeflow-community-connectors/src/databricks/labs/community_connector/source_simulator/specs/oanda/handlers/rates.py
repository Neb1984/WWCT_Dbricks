"""Custom handler for OANDA ``GET /v1/rates/{base}.{ext}``.

The Rates endpoint returns ``quotes`` as an object keyed by quote currency,
and which value fields appear depends on the ``fields`` and ``data_set``
query params. The declarative spec can't express that, so this handler
builds the response from the flat corpus rows (one row per quote currency):

* the requested ``base`` and ``date`` are stamped onto every quote, so the
  same corpus serves any base currency and any day;
* ``quote=`` (repeatable) filters the quote currencies; unknown ones are
  reported in ``meta.skipped_currencies`` like the live API;
* ``fields=`` selects bid/ask (averages), midpoint, highs, lows or all;
* Central Bank data sets (anything other than ``OANDA`` / ``CRYPTO``) return
  ``spot`` only and ignore ``fields`` (per the API docs);
* values are rounded to ``decimal_places``;
* invalid ``ext``, malformed or future ``date`` → 400 ``code: 1``.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_PATH_RE = re.compile(r"/v1/rates/(?P<base>[^/.]+)\.(?P<ext>[A-Za-z]+)$")
_NON_CENTRAL_BANK_DATA_SETS = {"OANDA", "CRYPTO"}
_FIELD_GROUPS = {
    "averages": ["bid", "ask"],
    "midpoint": ["midpoint"],
    "highs": ["high_bid", "high_ask"],
    "lows": ["low_bid", "low_ask"],
}
_FIELD_GROUPS["all"] = [f for group in _FIELD_GROUPS.values() for f in group]


def _json_response(status: int, body: Dict[str, Any], prep: PreparedRequest) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "X-Rate-Limit-Remaining": "unlimited",
        },
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def _bad_request(prep: PreparedRequest) -> Response:
    return _json_response(
        400,
        {
            "code": 1,
            "message": (
                "Invalid or malformed argument\n\nThe argument specified is not "
                "properly formatted or is an unaccepted value"
            ),
        },
        prep,
    )


def _round(value: Any, places: int) -> Any:
    if value is None:
        return None
    try:
        dec = Decimal(str(value))
    except InvalidOperation:
        return None
    quantum = Decimal(1).scaleb(-places)
    return str(dec.quantize(quantum, rounding=ROUND_HALF_EVEN))


def serve_rates(prep: PreparedRequest, spec, corpus) -> Response:
    parsed = urlparse(prep.url)
    match = _PATH_RE.search(parsed.path)
    if not match or match.group("ext").lower() != "json":
        return _bad_request(prep)
    base = match.group("base").upper()

    query = parse_qs(parsed.query)
    today = datetime.now(timezone.utc).date()
    raw_date = (query.get("date") or [today.isoformat()])[0]
    try:
        req_date = date.fromisoformat(raw_date)
    except ValueError:
        return _bad_request(prep)
    if req_date > today:
        return _bad_request(prep)

    data_set = (query.get("data_set") or ["OANDA"])[0].upper()
    raw_places = (query.get("decimal_places") or ["5"])[0]
    if not raw_places.isdigit() or not 1 <= int(raw_places) <= 15:
        return _bad_request(prep)
    places = int(raw_places)

    fields = [f.lower() for f in (query.get("fields") or ["averages"])]
    if any(f not in _FIELD_GROUPS for f in fields):
        return _bad_request(prep)
    wanted: List[str] = []
    for f in fields:
        for name in _FIELD_GROUPS[f]:
            if name not in wanted:
                wanted.append(name)
    if data_set not in _NON_CENTRAL_BANK_DATA_SETS:
        wanted = ["spot"]  # ``fields`` is ignored for Central Bank data sets

    requested_quotes = [q.upper() for q in query.get("quote") or []]

    rows: List[Dict[str, Any]] = corpus.get(spec.corpus) or []
    quotes: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ccy = row.get("quote_currency")
        if not ccy or ccy == base or ccy in quotes:
            continue
        if requested_quotes and ccy not in requested_quotes:
            continue
        quote: Dict[str, Any] = {"date": f"{req_date.isoformat()}T23:59:59+0000"}
        for name in wanted:
            value = _round(row.get(name), places)
            if value is not None:
                quote[name] = value
        quotes[ccy] = quote

    # Live API echoes ``fields`` normalized: ``all`` expanded to its groups,
    # de-duplicated and sorted (verified in record mode, 2026-10-29).
    groups = set(fields)
    if "all" in groups:
        groups = {"averages", "midpoint", "highs", "lows"}
    effective_fields = sorted(groups)

    effective_quotes = requested_quotes or sorted(quotes)
    skipped = [q for q in requested_quotes if q not in quotes]

    body = {
        "base_currency": base,
        "meta": {
            "effective_params": {
                "data_set": data_set,
                "date": req_date.isoformat(),
                "decimal_places": str(places),
                "fields": effective_fields,
                "quote_currencies": effective_quotes,
            },
            "request_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "skipped_currencies": skipped,
        },
        "quotes": quotes,
    }
    return _json_response(200, body, prep)
