# Smart Factory — Fog & Edge Computing CA Project

Scalable IoT architecture for a simulated smart factory: 3 virtual machines, 5 sensor types
each, processed by a virtual fog node and dispatched to a serverless AWS backend with a
live dashboard.

## Architecture

```
[Sensor Simulator]  --MQTT (local)-->  [Fog Node]  --MQTT/TLS-->  [AWS IoT Core]
 3 machines x 5                        filter/aggregate/                |
 sensor types                          anomaly-detect/batch        Rules Engine
 (Python, paho-mqtt)                   local retry buffer               |
                                                                  [Lambda: ingest]
                                                            /            |            \
                                    [DynamoDB]      [Firehose -> S3]   [SNS alerts]
                                    SmartFactory-SensorData   data lake        email
                                          |
                                    [Lambda: api]  <--  [API Gateway HTTP API]
                                          |
                                   [Dashboard] (static HTML/Chart.js, polls every 20s)
```

Scalability mechanisms: serverless fan-out via IoT Rules Engine + Lambda (auto-scales with
message volume, no servers to provision), DynamoDB on-demand capacity + GSI (avoids the hot
partition/throughput ceiling demonstrated in Lab 4), and Firehose buffering to decouple
ingestion rate from S3 write rate.

## Folder structure

```
sensors/    sensor simulator (runs anywhere, or on the Lab 1/2 EC2 instance)
fog/        fog node (runs on the same EC2 instance, forwards to AWS IoT Core)
backend/    Lambda source, DynamoDB schema, IoT Rule definition
dashboard/  static single-file dashboard
deploy/     AWS CLI setup script
docs/       report outline mapped to this architecture
```

## Prerequisites

- AWS Academy Learner Lab session started, credentials active
- Completed Lab 1 & 2 setup: EC2 instance with Mosquitto + paho-mqtt, IoT Thing
  `SmartFactory-fog-node-01` with certificates under `~/iot-lab1/certs/`, and your AWS IoT endpoint
- AWS CLI configured on that EC2 instance (or Cloud9) with the rotated Learner Lab credentials

## Run order

### 1. Local test (no AWS needed) — sanity-check sensors + fog logic

```bash
cd sensors && pip install -r requirements.txt
python simulator.py &

cd ../fog && pip install -r requirements.txt
# fog_node.py will fail to reach AWS until step 3, that's expected —
# it will buffer batches locally (SQLite) and print [fog][buffer] messages.
python fog_node.py
```

### 2. Provision the AWS backend

```bash
cd deploy
# edit setup_aws.sh: set YOUR_EMAIL and confirm REGION matches your Learner Lab region
bash setup_aws.sh
```

This creates: DynamoDB table + GSI, S3 bucket, Firehose delivery stream, SNS topic,
`SmartFactory-ingest-processor` Lambda, `SmartFactory-api` Lambda, the IoT topic rule, and an API Gateway
HTTP API. It prints the API base URL at the end — copy it.

Confirm the SNS email subscription from your inbox so anomaly alerts arrive.

### 3. Point the fog node at AWS IoT Core

Edit `fog/fog_config.yaml`:
- `aws_iot_endpoint`: from IoT Core → Settings (same endpoint as Lab 2)
- `ca_cert_path` / `cert_path` / `key_path`: point at your Lab 2 `certs/` folder

Re-run `python fog_node.py`. You should see `[fog][dispatch] -> factory/<device>/agg`
messages, and any buffered batches from step 1 will flush automatically.

### 4. Open the dashboard

The dashboard is hosted live on S3 as a static website:

http://smartfactory-datalake-712460979750.s3-website-us-east-1.amazonaws.com/dashboard/index.html

Paste the API Gateway base URL from step 2 into the "Connect" field (only needed once, it
is remembered on that browser via localStorage). Charts and stat cards populate within
~20 seconds and refresh continuously.

To host it yourself (e.g. after redeploying to a different AWS account), upload
`dashboard/index.html` to an S3 bucket with static website hosting enabled and a public-read
bucket policy scoped to that object, then browse to the buckets website endpoint. For local
development only, you can instead open `dashboard/index.html` directly in a browser
(double-click, or `python -m http.server` in that folder) and connect it to the same API
Gateway URL.

## Demonstrating anomalies end-to-end

The simulator injects out-of-range readings at a configurable rate (`anomaly_probability` in
`sensors/config.yaml`, default 3%). Trace one through the whole pipeline: dashboard shows the
point in red, the ingest Lambda's CloudWatch log shows the SNS publish, and you receive the
alert email — this is a good live-demo moment for the "difficult part / how you solved it"
question in the presentation.

## Troubleshooting

- **Fog node can't connect to AWS IoT Core**: check the cert paths and that the IoT policy
  attached to your certificate allows `iot:Publish` on `factory/*` (same policy pattern as
  Lab 2).
- **API Gateway returns 500**: check the `SmartFactory-api` Lambda's CloudWatch logs — usually a
  missing `SensorTypeIndex` GSI (still building) or wrong table name env var.
- **AWS Academy credentials expired**: Learner Lab credentials rotate each session — restart
  the lab, re-run `aws configure` (or refresh `~/.aws/credentials`) before re-running
  `setup_aws.sh` or the fog node.
