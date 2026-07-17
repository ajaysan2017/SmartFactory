"""
SmartFactory-api (Lambda)
--------------------
Backs an API Gateway HTTP API (payload format 2.0) that the dashboard polls.
Read-only, all queries hit DynamoDB (SmartFactory-SensorData + SensorTypeIndex GSI).

Routes (configure these in API Gateway -> Integrations, see deploy/setup_aws.sh):
  GET /readings                          -> latest N readings per sensor type (all devices)
  GET /device/{device_id}/history        -> a device's history, optional ?sensor_type=&hours=
  GET /stats                             -> avg/min/max/anomaly_count per sensor type

CORS: the dashboard is a static page opened from S3/local file, so this
Lambda sets Access-Control-Allow-Origin: * on every response.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "SmartFactory-SensorData")
GSI_NAME = os.environ.get("SENSOR_TYPE_INDEX", "SensorTypeIndex")
table = dynamodb.Table(TABLE_NAME)

SENSOR_TYPES = ["temperature", "vibration", "humidity", "energy_consumption", "machine_rpm"]

HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
}


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def _response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": HEADERS,
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def _get_readings(limit_per_type: int):
    result = {}
    for sensor_type in SENSOR_TYPES:
        resp = table.query(
            IndexName=GSI_NAME,
            KeyConditionExpression=Key("sensor_type").eq(sensor_type),
            ScanIndexForward=False,   # newest first
            Limit=limit_per_type,
        )
        result[sensor_type] = resp.get("Items", [])
    return result


def _get_device_history(device_id: str, sensor_type: str | None, hours: int):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    if sensor_type:
        key_cond = Key("device_id").eq(device_id) & Key("sk").begins_with(f"{sensor_type}#")
    else:
        key_cond = Key("device_id").eq(device_id)

    resp = table.query(KeyConditionExpression=key_cond, ScanIndexForward=False, Limit=200)
    items = [i for i in resp.get("Items", []) if i.get("timestamp", "") >= cutoff]
    return items


def _get_stats():
    stats = {}
    for sensor_type in SENSOR_TYPES:
        resp = table.query(
            IndexName=GSI_NAME,
            KeyConditionExpression=Key("sensor_type").eq(sensor_type),
            ScanIndexForward=False,
            Limit=50,
        )
        items = resp.get("Items", [])
        values = [float(i["value"]) for i in items]
        anomaly_count = sum(1 for i in items if i.get("anomaly"))
        stats[sensor_type] = {
            "count": len(values),
            "avg": round(sum(values) / len(values), 2) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "anomaly_count": anomaly_count,
        }
    return stats


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method == "OPTIONS":
        return _response(200, {})

    raw_path = event.get("rawPath", "/")
    path_params = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}

    try:
        if raw_path == "/readings":
            limit = int(query.get("limit", 5))
            return _response(200, _get_readings(limit))

        if raw_path.startswith("/device/") and raw_path.endswith("/history"):
            device_id = path_params.get("device_id") or raw_path.split("/")[2]
            sensor_type = query.get("sensor_type")
            hours = int(query.get("hours", 1))
            return _response(200, {"device_id": device_id,
                                    "items": _get_device_history(device_id, sensor_type, hours)})

        if raw_path == "/stats":
            return _response(200, _get_stats())

        return _response(404, {"error": f"unknown route {raw_path}"})

    except Exception as e:
        print(f"[api] error handling {raw_path}: {e}")
        return _response(500, {"error": str(e)})
