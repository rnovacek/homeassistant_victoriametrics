# VictoriaMetrics integration for Home Assistant

Exports entity state changes and numeric attributes to VictoriaMetrics using its
JSON import API. String attributes become metric labels. Metric names follow
`ha.<entity_id>.<attribute>` (for example `ha.sensor.temperature.value`).

## Installation

1. Copy the entire `custom_components/victoriametrics` directory into your Home
   Assistant configuration's `custom_components` directory.
2. Add the configuration below to `configuration.yaml`.
3. Restart Home Assistant.

```yaml
victoriametrics:
  url: http://localhost:8428
  prefix: ha
```

The URL is the VictoriaMetrics base URL; the integration appends `/api/v1/import`.
No Graphite listener is needed. VictoriaMetrics may be offline when Home Assistant
starts: collection starts immediately and delivery retries in the background.

## Configuration

| Option | Default | Description |
| --- | --- | --- |
| `url` | `http://localhost:8428` | VictoriaMetrics base URL. |
| `prefix` | `ha` | Metric name prefix. |
| `max_queue_records` | `100000` | Maximum pending metric records. One event can produce multiple records. |
| `max_queue_bytes` | `67108864` | Maximum pending JSON payload size (64 MiB). SQLite indexes, free pages, and transaction journals use additional disk space. |
| `batch_size` | `500` | Maximum records per request (1–10000). |
| `flush_interval` | `1` | Seconds between batches or idle checks (minimum 0.1). |

## Failure recovery and buffering

Metrics are committed to `.storage/victoriametrics.sqlite3` in the HA configuration
directory before transmission. Keep this file to preserve pending history across
restarts. It is separate from HA's recorder database. SQLite reuses freed pages;
its file can remain at its previous high-water size after the queue drains.

- Successful HTTP 2xx responses remove the delivered records.
- Connection failures, five-second total request timeouts, HTTP 408, 429, and 5xx
  retain records and retry with exponential backoff and jitter, up to 60 seconds.
- `Retry-After` seconds and HTTP dates are honored, including delays over 60 seconds.
- HTTP 401/403 report `authentication_error` and retry no more often than every
  five minutes. Other unexpected HTTP statuses also retain data and retry slowly.
- HTTP 400/413 responses cause batches to be split. A record still rejected on its
  own is dropped and counted so other records can continue.
- When either queue limit is exceeded, the oldest queued records are dropped and
  counted. Limits apply when records are appended, including after configuration
  changes. A record larger than the byte limit cannot be retained.

Shutdown stops accepting events, finishes outstanding disk writes, interrupts
retry sleeps, and allows the sender up to ten seconds to finish its current
operation. It leaves the remaining queue on disk instead of waiting for a full
network flush. An abrupt process crash before an event's disk commit can still
lose that event; storage failures are logged and counted as lost events.

Retries preserve the exact payload and original event timestamps. Delivery is
**at least once** while records remain buffered: a timeout after server acceptance
or a crash before local acknowledgement can cause duplicate samples on replay.
This is not an exactly-once guarantee.

VictoriaMetrics' streaming import API can acknowledge a request without reporting
individual parsing errors to the client. Check the server logs and
`vm_rows_invalid_total` when diagnosing missing samples:
[VictoriaMetrics import documentation](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#how-to-import-time-series-data).

## Delivery status

`sensor.victoriametrics_delivery` shows `starting`, `connected`, `unavailable`,
`authentication_error`, `http_error`, or `storage_error`. `connected` means the
last request succeeded, not that a separate health probe has checked the server.
The status sensor is excluded from export to avoid a feedback loop.

Attributes include:

- `pending_records`, `pending_bytes`, and `oldest_record_age_seconds`.
- `last_successful_write` and `next_retry` (UTC timestamps).
- `failed_attempts`, `consecutive_failures`, and `last_error`.
- `dropped_records`: persistent count of queue evictions and rejected records.
- `lost_events`: conversion or persistence failures during this HA session.

Write/failure history is session-local; the pending queue and dropped-record count
survive restarts. Status refreshes after attempts and during waits, at least every
30 seconds with the default settings. Logs report failure transitions and recovery;
repeated failures and queue-overflow reminders are limited to once per five minutes.

## Tests

```sh
uv run --with aiohttp --with pytest --with ciso8601 --with voluptuous pytest -q tests
```

Tests exercise real SQLite persistence, an HTTP test server, retry/error handling,
and conversion/startup/shutdown with a lightweight Home Assistant harness. They do
not boot a full Home Assistant instance.
