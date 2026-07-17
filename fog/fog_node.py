"""
Fog Node - Smart Factory Project
-----------------------------------------
Sits between the sensor layer (local Mosquitto broker) and AWS IoT Core.

Pipeline per raw reading:
  1. FILTER      - drop readings outside hard sanity bounds (sensor glitches)
  2. AGGREGATE   - average raw readings per (device_id, sensor_type) over a
                   rolling time window (reduces payload volume/noise)
  3. ANOMALY     - flag aggregated values outside the normal operating range
  4. BATCH       - accumulate aggregated points and dispatch them together as
                   one JSON array payload (fewer, larger messages to the cloud)
  5. DISPATCH    - publish via MQTT/TLS to AWS IoT Core, using the same
                   certificate-based auth pattern as Lab 2's aws_iot_publisher.py
  6. RESILIENCE  - if AWS IoT Core is unreachable, batches are written to a
                   local SQLite retry queue and re-sent on a timer once the
                   connection recovers (nothing is silently dropped)

Usage:
    pip install paho-mqtt pyyaml
    python fog_node.py                      # uses fog_config.yaml in same folder
"""

import argparse
import json
import os
import sqlite3
import ssl
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import yaml
import paho.mqtt.client as mqtt


class RetryBuffer:
    """Local SQLite-backed queue for batches that failed to dispatch to AWS."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS pending ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "topic TEXT NOT NULL, "
                "payload TEXT NOT NULL, "
                "created_at TEXT NOT NULL)"
            )

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def enqueue(self, topic: str, payload: str):
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO pending (topic, payload, created_at) VALUES (?, ?, ?)",
                (topic, payload, datetime.now(timezone.utc).isoformat()),
            )

    def pending_batch(self, limit: int = 50):
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "SELECT id, topic, payload FROM pending ORDER BY id ASC LIMIT ?", (limit,)
            )
            return cur.fetchall()

    def remove(self, row_id: int):
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM pending WHERE id = ?", (row_id,))

    def count(self) -> int:
        with self._lock, self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]


class FogNode:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.aggregation_window = float(cfg["aggregation_window_sec"])
        self.batch_size = int(cfg["batch_size"])
        self.flush_interval = float(cfg["flush_interval_sec"])
        self.filter_bounds = cfg["filter_bounds"]
        self.normal_range = cfg["normal_range"]

        # buffers: {(device_id, sensor_type): [values...]}
        self._window_buffer = defaultdict(list)
        self._window_started_at = time.time()
        self._batch = []
        self._last_flush = time.time()
        self._lock = threading.Lock()

        self.retry_buffer = RetryBuffer(cfg["retry_db_path"])
        self.aws_connected = False

        # --- local (sensor-facing) MQTT client ---
        self.local_client = mqtt.Client(client_id="smartfactory-fog-node-local")
        self.local_client.on_message = self._on_local_message
        self.local_client.on_connect = self._on_local_connect

        # --- AWS IoT Core (cloud-facing) MQTT client ---
        self.aws_client = mqtt.Client(client_id=cfg["aws_iot_client_id"])
        self.aws_client.tls_set(
            ca_certs=cfg["ca_cert_path"],
            certfile=cfg["cert_path"],
            keyfile=cfg["key_path"],
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        self.aws_client.on_connect = self._on_aws_connect
        self.aws_client.on_disconnect = self._on_aws_disconnect

    # ---------- MQTT callbacks ----------
    def _on_local_connect(self, client, userdata, flags, rc):
        print(f"[fog] connected to local broker (rc={rc}), subscribing to "
              f"{self.cfg['local_topic_filter']}")
        client.subscribe(self.cfg["local_topic_filter"])

    def _on_aws_connect(self, client, userdata, flags, rc):
        self.aws_connected = rc == 0
        print(f"[fog] AWS IoT Core connection result: rc={rc} connected={self.aws_connected}")

    def _on_aws_disconnect(self, client, userdata, rc):
        self.aws_connected = False
        print(f"[fog] AWS IoT Core disconnected (rc={rc}), will buffer locally and retry")

    def _on_local_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return  # malformed message, drop

        device_id = payload.get("device_id")
        sensor_type = payload.get("sensor_type")
        value = payload.get("value")
        if device_id is None or sensor_type is None or value is None:
            return

        # ---- 1. FILTER ----
        lo, hi = self.filter_bounds.get(sensor_type, (float("-inf"), float("inf")))
        if not (lo <= value <= hi):
            print(f"[fog][filter] dropped out-of-bounds reading {sensor_type}={value} ({device_id})")
            return

        with self._lock:
            self._window_buffer[(device_id, sensor_type)].append(value)

    # ---------- aggregation / batching loop ----------
    def _aggregate_and_batch_tick(self):
        now = time.time()
        if now - self._window_started_at < self.aggregation_window:
            return

        with self._lock:
            window = self._window_buffer
            self._window_buffer = defaultdict(list)
            self._window_started_at = now

        for (device_id, sensor_type), values in window.items():
            if not values:
                continue
            avg_value = round(sum(values) / len(values), 2)

            # ---- 3. ANOMALY DETECTION ----
            lo, hi = self.normal_range.get(sensor_type, (float("-inf"), float("inf")))
            is_anomaly = not (lo <= avg_value <= hi)

            point = {
                "device_id": device_id,
                "sensor_type": sensor_type,
                "value": avg_value,
                "sample_count": len(values),
                "anomaly": is_anomaly,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            with self._lock:
                self._batch.append(point)
            if is_anomaly:
                print(f"[fog][anomaly] {device_id} {sensor_type}={avg_value} outside {lo}-{hi}")

        self._maybe_flush()

    def _maybe_flush(self):
        should_flush = False
        with self._lock:
            if len(self._batch) >= self.batch_size:
                should_flush = True
            elif self._batch and (time.time() - self._last_flush) >= self.flush_interval:
                should_flush = True

        if should_flush:
            with self._lock:
                to_send = self._batch
                self._batch = []
                self._last_flush = time.time()
            self._dispatch(to_send)

    # ---------- 5. DISPATCH + 6. RESILIENCE ----------
    def _dispatch(self, points: list):
        if not points:
            return
        # group by device so the topic reflects the originating machine
        by_device = defaultdict(list)
        for p in points:
            by_device[p["device_id"]].append(p)

        for device_id, device_points in by_device.items():
            topic = f"{self.cfg['aws_publish_topic_prefix']}/{device_id}/agg"
            payload = json.dumps(device_points)
            self._publish_or_buffer(topic, payload)

    def _publish_or_buffer(self, topic: str, payload: str):
        if self.aws_connected:
            result = self.aws_client.publish(topic, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"[fog][dispatch] -> {topic} ({len(payload)} bytes)")
                return
        # AWS unreachable (or publish failed) -> buffer locally for retry
        self.retry_buffer.enqueue(topic, payload)
        print(f"[fog][buffer] AWS unreachable, queued 1 batch for {topic} "
              f"(pending={self.retry_buffer.count()})")

    def _retry_loop(self):
        while True:
            time.sleep(self.cfg["retry_interval_sec"])
            if not self.aws_connected:
                continue
            for row_id, topic, payload in self.retry_buffer.pending_batch():
                result = self.aws_client.publish(topic, payload, qos=1)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.retry_buffer.remove(row_id)
                    print(f"[fog][retry] flushed queued batch -> {topic}")
                else:
                    break  # stop trying this round, connection likely dropped again

    def run(self):
        self.local_client.connect(self.cfg["local_broker"], self.cfg["local_port"], keepalive=60)
        self.local_client.loop_start()

        try:
            self.aws_client.connect(self.cfg["aws_iot_endpoint"], self.cfg["aws_iot_port"], keepalive=60)
            self.aws_client.loop_start()
        except Exception as e:
            print(f"[fog] initial AWS IoT Core connect failed: {e}. "
                  f"Will buffer locally and retry every {self.cfg['retry_interval_sec']}s.")

        threading.Thread(target=self._retry_loop, daemon=True).start()

        print("[fog] running. Ctrl+C to stop.")
        try:
            while True:
                self._aggregate_and_batch_tick()
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n[fog] stopping.")
        finally:
            self.local_client.loop_stop()
            self.aws_client.loop_stop()


def main():
    parser = argparse.ArgumentParser()
    default_cfg = os.path.join(os.path.dirname(__file__), "fog_config.yaml")
    parser.add_argument("--config", default=default_cfg)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    FogNode(cfg).run()


if __name__ == "__main__":
    main()
