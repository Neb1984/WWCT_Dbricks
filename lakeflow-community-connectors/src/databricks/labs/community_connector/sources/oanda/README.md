# Lakeflow OANDA Exchange Rates Community Connector

This documentation describes how to configure and use the **OANDA Exchange Rates** Lakeflow community connector to ingest daily foreign-exchange quotes from the OANDA Exchange Rates API into Databricks.

The connector uses the v1 **Rates** endpoint (`GET /v1/rates/{base}.json`) and exposes one table, `rates`, with one row per base currency, quote currency, UTC day and data set.

## Prerequisites

- **OANDA Exchange Rates API subscription**: a trial or paid plan for the OANDA Exchange Rates API (FXDS). Sign up at <https://www.oanda.com/fx-for-business/exchange-rates-api>.
- **API key**: available in the OANDA FXDS Self-Serve Portal (<https://app.corporatefxservices.com/app>). The connector sends it as `Authorization: Bearer <api_key>`.
- **Quote limits**: some plans limit the number of *quotes returned* per billing period (not the number of requests). Each `(quote currency, day)` the connector reads counts as one quote. Use `quote_currencies` to control cost.
- **Central Bank data sets** (optional): require a plan with Central Bank access. Without it the API returns HTTP 403.
- **Network access**: the pipeline compute must reach `https://api.corporatefxservices.com` (or your configured `base_url`) over HTTPS with TLS 1.2+.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_key` | string | yes | OANDA Exchange Rates API key. **Store as a Databricks secret.** | `<DATABRICKS_SECRET>` |
| `base_url` | string | no | API host without a trailing slash. Default `https://api.corporatefxservices.com`. The legacy host `https://web-services.oanda.com/rates/api` serves the same v1 API. | `https://api.corporatefxservices.com` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific options allowed to pass through to the connector. | `base_currency,quote_currencies,data_set,fields,decimal_places,start_date,initial_lookback_days,max_records_per_batch,max_days_per_batch` |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`base_currency,quote_currencies,data_set,fields,decimal_places,start_date,initial_lookback_days,max_records_per_batch,max_days_per_batch`

> **Note**: These options are **not** connection parameters. They are set per table in `table_configuration` in the pipeline spec, and their names must be listed in `externalOptionsAllowList`.

### Obtaining the Required Parameters

1. Sign in to the OANDA FXDS Self-Serve Portal at <https://app.corporatefxservices.com/app>.
2. Copy your **API key** from the portal home page (it is also in your registration email).
3. Store it in Databricks Secrets, for example `dbutils.secrets.get(scope="oanda", key="api_key")`, and reference the secret as the `api_key` connection option.

### Create a Unity Catalog Connection

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to the list above.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `rates` | Daily quotes (bid, ask, midpoint, highs, lows, or Central Bank spot) for one base currency against the requested quote currencies | `cdc` | `base_currency`, `quote_currency`, `rate_date`, `data_set` | `rate_date` |

### How incremental reads work

- The API has no "changed since" filter; quotes are addressed by date. The connector makes **one API call per UTC day** and walks forward from the stored cursor.
- **First run**: starts at `start_date`, or `initial_lookback_days` before today (default 30) if `start_date` is not set.
- **Later runs**: resume at the day after the last loaded `rate_date`.
- Only **complete UTC days** are loaded. The upper bound is yesterday (UTC) at the time the pipeline update starts, so a partial "today" quote is never written. Today's rates are picked up by the next run after 00:00 UTC.
- Each microbatch reads whole days only, up to `max_days_per_batch` days or until `max_records_per_batch` rows have been collected.
- If the API responds `404` with code `56` ("rates not yet published"), the connector stops the batch without moving past that day and retries it on the next run.
- `cdc` (upsert on the primary key) is used so a re-read day never creates duplicate rows. No deletes exist in the source.

### Multiple base currencies or data sets

A single `rates` table configuration reads one `base_currency` and one `data_set`. To ingest several, add one pipeline table entry per combination. Because `base_currency` and `data_set` are part of the primary key, they can share one destination table or write to separate ones.

### Schema notes

- `quotes` in the API response is a JSON object keyed by currency code; the connector flattens it into one row per quote currency.
- `rate_date` is the UTC day the connector requested. `quote_date` is the raw `date` the API returned for that quote. For currency pairs not updated daily the API returns the most recent earlier quote, so `quote_date` can be earlier than `rate_date`.
- Value columns are populated according to `fields`. `high_*` and `low_*` are only available for instruments traded on OANDA fxTrade and are `NULL` otherwise.
- Central Bank data sets (any `data_set` other than `OANDA` or `CRYPTO`) return only `spot`; `fields` is ignored for them.
- Currencies with no data for a day are omitted by the API (no row is written).

## Table Configurations

### Source & Destination

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | `rates` |
| `destination_catalog` | No | Target catalog (defaults to the pipeline's default). |
| `destination_schema` | No | Target schema (defaults to the pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. |
| `primary_keys` | No | Override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |

### Source-specific `table_configuration` options (table `rates`)

| Option | Type | Default | Description |
|---|---|---|---|
| `base_currency` | string | `USD` | Base currency code. Case-insensitive. |
| `quote_currencies` | comma-separated string | all available | Quote currencies, e.g. `EUR,GBP,JPY`. If not set, the API returns every available quote (about 200 per day), and each one counts against quote-limited plans. **Recommended to set.** |
| `data_set` | string | `OANDA` | `OANDA`, `CRYPTO`, or a Central Bank data set code such as `EUCB` or `USFR`. |
| `fields` | comma-separated string | `all` | Any of `averages` (bid/ask), `midpoint`, `highs`, `lows`, `all`. |
| `decimal_places` | integer | `5` | 1 to 15. (`all` is not supported because the destination type has a fixed scale of 15.) |
| `start_date` | `YYYY-MM-DD` | none | First day to load on the initial run. Takes precedence over `initial_lookback_days`. |
| `initial_lookback_days` | integer | `30` | Days before today to start from when `start_date` is not set. |
| `max_records_per_batch` | integer | `1000` | Soft cap on rows per microbatch. Whole days only: the batch stops after the day that reaches the cap. |
| `max_days_per_batch` | integer | `31` | Maximum days (API calls) per microbatch. |

## Data Type Mapping

| API field | Example | Connector column | Spark type | Notes |
|---|---|---|---|---|
| `base_currency` | `USD` | `base_currency` | `StringType` | |
| key of `quotes` | `EUR` | `quote_currency` | `StringType` | |
| request `date` | `2026-10-28` | `rate_date` | `DateType` | UTC. |
| `meta.effective_params.data_set` | `OANDA` | `data_set` | `StringType` | |
| `quotes.<CCY>.date` | | `quote_date` | `StringType` | Raw value; the API spec does not define the format. |
| `bid`, `ask`, `midpoint`, `high_bid`, `high_ask`, `low_bid`, `low_ask`, `spot` | `"0.92010"` | same names | `DecimalType(38, 15)` | The API returns decimal strings to avoid rounding errors. |
| `meta.effective_params.decimal_places` | `"5"` | `decimal_places` | `StringType` | |
| `meta.request_time` | | `request_time` | `StringType` | UTC time the API served the request. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the OANDA connector source in your workspace.

### Step 2: Configure Your Pipeline

Example `pipeline_spec` that loads USD- and EUR-based rates for a few currencies since the start of 2026:

```json
{
  "pipeline_spec": {
    "connection_name": "oanda_connection",
    "object": [
      {
        "table": {
          "source_table": "rates",
          "destination_table": "fx_rates",
          "table_configuration": {
            "base_currency": "USD",
            "quote_currencies": "EUR,GBP,JPY,CHF,CAD",
            "fields": "averages,midpoint",
            "start_date": "2026-01-01"
          }
        }
      },
      {
        "table": {
          "source_table": "rates",
          "destination_table": "fx_rates_eur",
          "table_configuration": {
            "base_currency": "EUR",
            "quote_currencies": "USD,GBP",
            "start_date": "2026-01-01"
          }
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline once to backfill from `start_date`, then schedule it daily, shortly after 00:00 UTC, to pick up the previous day's quotes.

#### Best Practices

- **Always set `quote_currencies`** on quote-limited plans. A backfill of N days times Q currencies costs N x Q quotes.
- **Backfill in chunks**: large `start_date` ranges make one API call per day; use `max_days_per_batch` to keep microbatches small.
- **Schedule after 00:00 UTC**: rates for a day are published at 00:00 UTC the next day.

#### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `HTTP 401` code 3 | Missing or wrong `api_key`. |
| `HTTP 400` code 8 | Malformed key or Authorization header. |
| `HTTP 403` code 4 | Plan does not include the requested data set (for example, Central Bank rates). |
| `HTTP 403` code 53 / 54 | Quote limit for the billing period reached or would be exceeded. Narrow `quote_currencies` or contact OANDA. |
| `HTTP 400` code 1 | Invalid option value (currency code, `data_set`, date). |
| No rows for some currencies | No quotes exist for that pair or date (for example, EUR before 1999). |

## References

- OANDA Exchange Rates API documentation: <https://exchange-rates-api.oanda.com/>
- OpenAPI specification: <https://exchange-rates-api.oanda.com/openapi.json>
- Connector API research notes: `oanda_api_doc.md`
