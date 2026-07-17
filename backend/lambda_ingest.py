"""
SmartFactory-ingest-processor (Lambda)
--------------------------------
Triggered by the AWS IoT Core Rules Engine for every message published by the
fog node to topic `factory/<device_id>/agg`. The message body is a JSON array
of aggregated sensor points (see fog/fog_node.py).

This single Lambda fans the batch out to THREE destinations, mirroring the
Lab 4 "one message, three destinations" pattern:
  1. DynamoDB   - hot storage for the dashboard/API  (SmartFactory-SensorData)
  2. Firehose   - S3 data lake for later analytics    (SmartFactory-datalake-stream)
  3. SNS        - real-time alert if any point in the batch is anomalous
                  (SmartFactory-anomaly-alerts)

Environment variables (set on the Lambda, see deploy/setup_aws.sh):
  DYNAMODB_TABLE      default: SmartFactory-SensorData
  FIREHOSE_STREAM     default: SmartFactory-datalake-stream
  SNS_TOPIC_ARN       required for alerting

This is exactly the kind of stateless, auto-scaling fan-out AWS Lambda is
designed for: no server to provision, and it scales automatically with the
number of fog nodes/messages (the "scalability mechanism" for the backend).
"""

import json
import os
import time
from decimal import Decimal

import boto3

dynamodb = boto3.resource("dynamodb")
firehose = boto3.client("firehose")
sns = boto3.client("sns")

TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "SmartFactory-SensorData")
STREAM_NAME = os.environ.get("FIREHOSE_STREAM", "SmartFactory-datalake-stream")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

table = dynamodb.Table(TABLE_NAME)


def _to_decimal(obj):
    """DynamoDB's boto3 resource requires Decimal instead of float."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_decimal(v) for v in obj]
    return obj


def handler(event, context):
    # IoT rule delivers the raw MQTT payload as the Lambda event (already
    # decoded from JSON by the rules engine's default JSON action config).
    points = event if isinstance(event, list) else event.get("points", [])
    if not points:
        print("[ingest] empty payload, nothing to do")
        return {"processed": 0}

    written = 0
    anomalies = []
    firehose_records = []

    with table.batch_writer() as batch:
        for point in points:
            device_id = point.get("device_id")
            sensor_type = point.get("sensor_type")
            timestamp = point.get("timestamp")
            if not (device_id and sensor_type and timestamp):
                continue

            item = {
                "device_id": device_id,
                "sk": f"{sensor_type}#{timestamp}",
                "timestamp": timestamp,
                "sensor_type": sensor_type,
                "value": _to_decimal(point.get("value")),
                "sample_count": int(point.get("sample_count", 1)),
                "anomaly": bool(point.get("anomaly", False)),
            }
            batch.put_item(Item=item)
            written += 1

            firehose_records.append({"Data": (json.dumps(point) + "\n").encode("utf-8")})

            if item["anomaly"]:
                anomalies.append(point)

    # 2. Archive raw batch to S3 via Firehose (best-effort; do not fail the
    #    whole invocation if the data lake is temporarily unavailable)
    if firehose_records:
        try:
            for i in range(0, len(firehose_records), 500):  # Firehose batch limit
                firehose.put_record_batch(
                    DeliveryStreamName=STREAM_NAME,
                    Records=firehose_records[i:i + 500],
                )
        except Exception as e:
            print(f"[ingest] Firehose put_record_batch failed: {e}")

    # 3. Real-time alert if any point in this batch is anomalous
    if anomalies and SNS_TOPIC_ARN:
        summary = "\n".join(
            f"- {p['device_id']} {p['sensor_type']}={p['value']} at {p['timestamp']}"
            for p in anomalies
        )
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="SmartFactory Anomaly Alert",
                Message=f"{len(anomalies)} anomalous reading(s) detected:\n{summary}",
            )
        except Exception as e:
            print(f"[ingest] SNS publish failed: {e}")

    print(f"[ingest] wrote {written} points, {len(anomalies)} anomalies, "
          f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    return {"processed": written, "anomalies": len(anomalies)}
