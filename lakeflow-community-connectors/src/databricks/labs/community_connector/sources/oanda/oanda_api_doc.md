# **OANDA Exchange Rates API Documentation**

Scope: the v1 **Rates** endpoint `GET /v1/rates/{base}.{ext}` of the OANDA
Exchange Rates API (FXDS / corporatefxservices). Other endpoints in the same
API (v2 spot/candles/aggregated/forward/stream, currencies, remaining quotes,
v3 order/position books) are out of scope for this connector iteration.

Primary source: the official OpenAPI 3 specification served by the docs site
`https://exchange-rates-api.oanda.com/openapi.json` (API spec version `2.0.0`,
rendered at <https://exchange-rates-api.oanda.com/>).

## **Authorization**

- Every endpoint requires an **API key**.
- Preferred method: HTTP header `Authorization: Bearer <API_KEY>`
  (OpenAPI `securitySchemes.apiKey`, `in: header`, `name: Authorization`).
- Deprecated alternative: `api_key=<API_KEY>` query parameter. Marked
  *Not recommended* (the key may end up in logs). The connector does **not**
  use it.
- The key is obtained from the OANDA FXDS Self-Serve Portal
  (<https://app.corporatefxservices.com/app>).
- TLS v1.2+ with SNI is enforced.

### Base URL

| Host | Notes |
|---|---|
| `https://api.corporatefxservices.com` | Production server declared in the OpenAPI `servers` block. Connector default. |
| `https://web-services.oanda.com/rates/api` | Legacy OANDA host serving the same v1 API (responds with header `x-oanda-exchange-rates-api-version`). Can be set via the `base_url` connection option. |

### Example authenticated request

```bash
curl -H "Authorization: Bearer <API_KEY>" \
  "https://api.corporatefxservices.com/v1/rates/USD.json?quote=EUR&quote=GBP&date=2026-10-28&fields=all"
```

### Auth failure behavior

| HTTP | code | message |
|---|---|---|
| 400 | 8 | Malformed Authorization header or invalid access token |
| 401 | 3 | This request requires authorization |
| 403 | 4 | The access token provided does not allow this request to be made (e.g. no access to Central Bank rates) |

Verified: an unauthenticated request to
`https://web-services.oanda.com/rates/api/v1/rates/USD.json?quote=EUR`
returns `401 {"code":3,"message":"This request requires authorization"}`.

## **Object List**

| Object (table) | Endpoint | Description |
|---|---|---|
| `rates` | `GET /v1/rates/{base}.{ext}` | Daily quotes (bid/ask/midpoint/highs/lows) for one base currency against one or more quote currencies, for a given date. |

The object list is static. A single API call returns the quotes of one
`base` currency for one `date` (or one averaged `start`..`end` range). The
connector models each `(base, quote, date, data_set)` combination as a row.

## **Object Schema**

### Raw response (JSON, `ext=json`)

```json
{
  "base_currency": "USD",
  "meta": {
    "effective_params": {
      "data_set": "OANDA",
      "date": "2026-10-28",
      "decimal_places": "5",
      "fields": ["averages", "midpoint", "highs", "lows"],
      "quote_currencies": ["EUR", "GBP"]
    },
    "request_time": "2026-10-29T08:00:00+0000",
    "skipped_currencies": []
  },
  "quotes": {
    "EUR": {"date": "...", "bid": "0.92010", "ask": "0.92030", "midpoint": "0.92020",
            "high_bid": "...", "high_ask": "...", "low_bid": "...", "low_ask": "..."},
    "GBP": {"date": "...", "bid": "...", "ask": "..."}
  }
}
```

The example above is shaped after the OpenAPI response schema (field names and
nesting are exact; values are illustrative).

Top-level fields (all `required` per the spec): `base_currency` (string),
`meta` (object), `quotes` (object keyed by quote currency code —
`additionalProperties`).

`meta`:

| Field | Type | Notes |
|---|---|---|
| `effective_params.data_set` | string enum | e.g. `OANDA`, `CRYPTO`, central-bank codes (`EUCB`, `USFR`, ...). Default `OANDA`. |
| `effective_params.date` / `start` / `end` | string `YYYY-MM-DD` | Echo of the normalized request. |
| `effective_params.decimal_places` | string | |
| `effective_params.fields` | array<string> | |
| `effective_params.quote_currencies` | array<string> | |
| `request_time` | string | UTC date-time the request was made. |
| `skipped_currencies` | array<string> | Quote currencies with no data for the date/range. |

`quotes.<CCY>` (per quote currency):

| Field | Type | Notes |
|---|---|---|
| `date` | string | **Required.** Timestamp of the quote actually used. For pairs not updated daily this can be *earlier* than the requested date. Omitted for date-range requests. Exact format TBD: not specified in the OpenAPI spec (string only). |
| `bid`, `ask` | Decimal-as-string | Returned when `fields` includes `averages` (default). |
| `midpoint` | Decimal-as-string | `fields=midpoint`. |
| `high_bid`, `high_ask` | Decimal-as-string | `fields=highs`; only for instruments traded on fxTrade; omitted when unavailable. |
| `low_bid`, `low_ask` | Decimal-as-string | `fields=lows`; same availability caveat. |
| `spot` | Decimal-as-string | Central Bank data sets only (described in the endpoint description; not in the response schema). `fields` is ignored for Central Bank data sets. |

Rates are encoded as strings "to prevent rounding issues"; up to 15 decimal
places (`decimal_places` enum `1`..`15`, or `all`).

### Connector (flattened) schema — table `rates`

| Column | Spark type | Source |
|---|---|---|
| `base_currency` | string | response `base_currency` |
| `quote_currency` | string | key of `quotes` map |
| `rate_date` | date | the `date` query parameter the connector requested (UTC day) |
| `data_set` | string | `meta.effective_params.data_set` (fallback: requested `data_set`) |
| `quote_date` | string | `quotes.<CCY>.date` (raw) |
| `bid`, `ask`, `midpoint`, `high_bid`, `high_ask`, `low_bid`, `low_ask`, `spot` | decimal(38,15) | `quotes.<CCY>.*` |
| `decimal_places` | string | `meta.effective_params.decimal_places` |
| `request_time` | string | `meta.request_time` |

## **Get Object Primary Keys**

No server-side primary key exists. The natural key of a quote is
`(base_currency, quote_currency, rate_date, data_set)`.

## **Object's Ingestion Type**

`rates`: **`cdc`** with cursor `rate_date`.

- Rates are posted daily at 00:00 UTC; a past date's quote does not change
  once published, so the data is effectively append-by-day.
- `cdc` (upsert on the natural key) is used instead of `append` so that
  re-reading a day (lookback, retries, re-run of the same day) does not
  duplicate rows.
- No deletes exist in the source.

## **Read API for Data Retrieval**

### Endpoint

`GET {base_url}/v1/rates/{base}.{ext}`

| Param | In | Required | Notes |
|---|---|---|---|
| `base` | path | yes | Uppercase base currency code. |
| `ext` | path | yes | `json` \| `csv` \| `xml`. Connector always uses `json` (CSV omits metadata). |
| `quote` | query (repeatable array) | no | Quote currency. Omitted ⇒ all available quotes (~200) are returned **and charged** against the plan's quote limit. Repeated as `quote=EUR&quote=GBP`. |
| `fields` | query (repeatable array) | no | `averages` (default), `midpoint`, `highs`, `lows`, `all`. |
| `data_set` | query | no | Default `OANDA`. |
| `decimal_places` | query | no | `1`..`15` or `all`; default `5`. |
| `date` | query | no | `YYYY-MM-DD`, UTC, `<= today`. Default: current day. |
| `start`, `end` | query | no | Averaged range. `start` without `end` ⇒ `end`=today. Not used by the connector (averages are not per-day rows). |
| `api_key` | query | no | Deprecated. |

### Pagination

None. One call returns every requested quote currency for one date.

### Incremental strategy

The API has no "changed since" filter; data is addressed by date. The
connector walks forward **one UTC day per API call**:

1. Starting cursor: prior offset `rate_date`, else the `start_date` table
   option, else `today - initial_lookback_days` (default 30).
2. For each day `d` from `cursor + 1` (or `start_date` on first run) up to
   the init-time cap (the UTC date at connector init **minus one day**, i.e.
   only complete days), call `?date=d` and emit one row per quote.
3. Stop accumulating once `max_records_per_batch` is reached (whole days only).
4. Offset = `{"rate_date": <last day read>}`. When the cursor reaches the
   init-time cap, return the same offset (termination).

Today's rates may be incomplete/unpublished early in the UTC day; the API
returns `404 code 56 "The rates requested have not yet been published"`.
The connector treats 404/56 as "no data yet" and stops the batch without
advancing past that day.

### Rate limits / quota

- No request-rate limit is documented. Plans may have a **quote limit**:
  every quote returned counts. The `X-Rate-Limit-Remaining` response header
  (integer or `unlimited`) reports the remaining quotes in the billing period.
- 403 code 53: quote allocation exceeded. 403 code 54: request would exceed
  remaining quotes. These are non-retriable.
- Date-range requests: `#quote currencies × #days <= 10,000` (400 code 55).
- Recommended: always pass an explicit `quote` list to control cost.

### Error codes (Rates endpoint)

| HTTP | code | meaning |
|---|---|---|
| 400 | 1 | Invalid or malformed argument |
| 400 | 2 | Missing required argument |
| 400 | 8 | Malformed Authorization header / invalid token |
| 400 | 55 | Too many currency-pair × date combinations |
| 401 | 3 | Authorization required |
| 403 | 4 / 53 / 54 | Not permitted / quote limit exceeded |
| 404 | 56 | Rates not yet published |

### Special cases (from the endpoint description)

- All dates/times are UTC.
- Absent quotes for a date are simply omitted (and listed in
  `meta.skipped_currencies`); e.g. EUR pairs before 1999-01-01.
- Pairs not updated daily return the most recent quote `<=` the requested
  date (hence `quote_date` may differ from `rate_date`).
- If only highs/lows are requested and unavailable, the quote is omitted;
  if all are omitted the API errors.

## **Field Type Mapping**

| API type | Spark type |
|---|---|
| currency code string | StringType |
| Decimal-as-string | DecimalType(38, 15) |
| `YYYY-MM-DD` (requested date) | DateType |
| date-time strings (`quote_date`, `request_time`) | StringType (format not specified by the spec) |

## **Known Quirks & Edge Cases**

- `quotes` is a JSON object keyed by currency code, not an array.
- `fields` is ignored for Central Bank data sets, which return `spot` only.
- Not passing `quote` can be expensive on quote-limited plans.

## **Research Log**

| Source | URL | Used for |
|---|---|---|
| Official OpenAPI spec (v2.0.0) | https://exchange-rates-api.oanda.com/openapi.json | Endpoint, params, response schema, errors, auth, servers |
| Official docs (RapiDoc render of the same spec) | https://exchange-rates-api.oanda.com/ | Cross-check |
| Unauthenticated probe | https://web-services.oanda.com/rates/api/v1/rates/USD.json | Confirmed 401/code 3 behavior and legacy host |

Verified in record mode with a live key (2026-10-29): `quotes.<CCY>.date` is
`YYYY-MM-DDT23:59:59+0000` for daily quotes; `meta.request_time` is
`YYYY-MM-DDTHH:MM:SS+0000`; `fields=all` is echoed expanded and sorted
(`["averages","highs","lows","midpoint"]`); `X-Rate-Limit-Remaining` is an
integer on quote-limited plans. Still TBD: presence of `spot` for Central
Bank data sets in v1 JSON (not exercised; requires Central Bank entitlement).
