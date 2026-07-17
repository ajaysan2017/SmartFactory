# Report outline — IEEE 2-column, 6–8 pages

Use the official IEEE Conference template. Sections below map directly onto what's built in
this repo, so each one has a short list of points to write from — fill in with your own
numbers/screenshots once you've run the demo.

## Abstract
One paragraph: smart-factory IoT scenario, fog node (filter/aggregate/anomaly-detect/batch),
serverless AWS backend (IoT Core → Lambda → DynamoDB/S3/SNS), live dashboard. State the
headline result (e.g. "processes N msgs/sec end-to-end with a live dashboard update within
~20s").

## Introduction
- Domain: factory floor monitoring, 3 machines, 5 sensor types (temperature, vibration,
  humidity, energy consumption, RPM).
- Objective: demonstrate a scalable fog-to-cloud pipeline that survives the limitations
  found in Lab 4 (single-table throughput, no real-time alerting, no cold storage) at higher
  device counts.
- Requirements: configurable sensor frequency, local processing before cloud dispatch,
  resilience to backend outages, auto-scaling backend, responsive dashboard.

## Architecture and design
- Include the diagram from README.md (redraw it cleanly for the report).
- Justify: why aggregate at the fog layer (bandwidth reduction, noise smoothing) instead of
  streaming every raw reading to the cloud.
- Justify: why DynamoDB + GSI over RDS (see `backend/dynamodb_schema.md` — ties directly to
  the Lab 4 "DynamoDB Limitation" material, cite it as your baseline).
- Justify: why a single fan-out Lambda (`SmartFactory-ingest-processor`) rather than three separate
  native IoT Rule actions — payloads arrive batched as JSON arrays, and only Lambda can
  iterate/split a batch before writing to DynamoDB/Firehose/SNS.
- Scalability mechanism: Lambda auto-scaling on message volume, DynamoDB on-demand capacity,
  Firehose buffering (decouples burst ingestion from S3 write rate), API Gateway HTTP API
  (cheaper/faster than REST API, sufficient for this read pattern).
- Resilience: local SQLite retry buffer in the fog node (`fog/fog_node.py::RetryBuffer`) —
  discuss what happens if AWS IoT Core is unreachable and how it recovers.

## Implementation
- Software components: list each file/module (sensors/simulator.py, fog/fog_node.py,
  backend/lambda_ingest.py, backend/lambda_api.py, dashboard/index.html) with 1–2 lines each.
- Libraries: paho-mqtt (MQTT client), boto3 (AWS SDK), Chart.js (dashboard visualization).
- Deployment: `deploy/setup_aws.sh` — AWS CLI provisioning script; mention this is
  Infrastructure-as-Script (discuss whether you'd move to CloudFormation/Terraform given more
  time — good CA reflection point).
- Include your GitHub repo link here.
- If you add CI (e.g. a GitHub Action that lints the Python files with `flake8` on push),
  document it here — it directly answers the "continuous integration" requirement in the
  brief.

## Conclusions
- What worked, what was hardest (candidates: TLS cert wiring for the fog→IoT Core
  connection, splitting batched array payloads inside the IoT Rule, DynamoDB key design for
  two access patterns at once).
- What you'd do differently at real scale (e.g. move from a single EC2 fog node to a fog
  cluster with load-balanced MQTT ingestion, or introduce Kinesis Data Streams ahead of
  Lambda per Lab 4's optional exercise if the write rate exceeded Lambda's concurrency).
- Short personal reflection paragraph (required by the brief).

## References
Cite in IEEE style: AWS IoT Core docs, AWS Lambda docs, DynamoDB docs, Kinesis Firehose docs,
paho-mqtt docs, Chart.js docs, plus any academic papers on fog computing architectures you
draw on for the "critically analyse" requirement in the Introduction/Architecture sections.
Remember: reused lab code MUST be cited (the certificate/MQTT connection pattern in
`fog/fog_node.py` is adapted from Lab 2's `aws_iot_publisher.py` — say so explicitly).

## Presentation (max 4 minutes)
1. ~30s: motivation + architecture diagram
2. ~2 min: live demo — start simulator, show dashboard populating, trigger an anomaly, show
   the SNS email arrive
3. ~1 min: hardest part + how you solved it (pick one from Conclusions above)
4. ~30s: wrap-up / scalability argument
