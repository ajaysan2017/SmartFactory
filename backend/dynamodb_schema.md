# DynamoDB Schema — SmartFactory-SensorData

Single table, on-demand (pay-per-request) capacity mode.

| Key | Attribute | Type | Notes |
|---|---|---|---|
| Partition key | `device_id` | String | e.g. `machine-01` |
| Sort key | `sk` | String | `"<sensor_type>#<timestamp_iso>"`, e.g. `temperature#2026-07-17T10:15:03.120Z` |
| Attribute | `timestamp` | String (ISO-8601) | when the fog node aggregated the reading |
| Attribute | `sensor_type` | String | one of: temperature, vibration, humidity, energy_consumption, machine_rpm |
| Attribute | `value` | Number | aggregated (averaged) sensor value |
| Attribute | `sample_count` | Number | how many raw readings were averaged into this point |
| Attribute | `anomaly` | Boolean | true if the fog node flagged this point outside normal range |

Sort key design (`sensor_type#timestamp`) lets `GET /device/{id}/history?sensor_type=X` use a cheap
`begins_with(sk, "X#")` query instead of a filter/scan.

## Global Secondary Index — `SensorTypeIndex`

| Key | Attribute | Type |
|---|---|---|
| Partition key | `sensor_type` | String |
| Sort key | `timestamp` | String (ISO-8601) |

Used for cross-device queries: "latest N readings of sensor type X across all machines" (powers
`GET /readings` and `GET /stats` without a full table scan — this is the scalability-relevant design
choice called out in the report's architecture section).

## Why DynamoDB (vs. RDS)

Time-series IoT writes are high-volume, append-only, and queried almost exclusively by
`device_id`/`sensor_type` + time range — a key-value/wide-column access pattern DynamoDB serves
natively with on-demand auto-scaling and no server management, unlike RDS which would need explicit
instance sizing and read-replica provisioning to absorb the same write burst (this is also the exact
limitation demonstrated in Lab 4).
