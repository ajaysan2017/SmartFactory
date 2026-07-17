"""
Sensor Layer - Smart Factory Project
--------------------------------------------
Simulates 5 sensor types (temperature, vibration, humidity, energy_consumption,
machine_rpm) across N virtual machines on a factory floor, and publishes each
reading as JSON to a LOCAL Mosquitto broker (matches Lab 1/2 setup).

Each reading follows a sine-wave baseline (models daily/cyclical load patterns)
plus Gaussian noise, and readings are occasionally pushed out of range to
simulate faults (anomaly injection) so the fog layer and backend alerting can
be demonstrated end-to-end.

Usage:
    pip install paho-mqtt pyyaml
    python simulator.py                      # uses config.yaml in same folder
    python simulator.py --config myconf.yaml # custom config
    PUBLISH_INTERVAL=1 python simulator.py   # override frequency via env var

Topic scheme (local broker):
    factory/<device_id>/<sensor_type>/raw
"""

import argparse
import json
import math
import os
import random
import time
from datetime import datetime, timezone

import yaml
import paho.mqtt.client as mqtt


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def generate_reading(sensor_name: str, cfg: dict, t: float, inject_anomaly: bool) -> dict:
    """Sine-wave baseline + noise, optionally shifted into an anomalous range."""
    baseline = cfg["baseline"]
    amplitude = cfg["amplitude"]
    noise_std = cfg["noise_std"]

    # slow sine wave (period ~ 5 minutes) models normal cyclical load
    wave = amplitude * math.sin(2 * math.pi * t / 300.0)
    noise = random.gauss(0, noise_std)
    value = baseline + wave + noise

    if inject_anomaly:
        value += cfg["anomaly_offset"]

    return {
        "value": round(value, 2),
        "unit": cfg["unit"],
    }


def main():
    parser = argparse.ArgumentParser()
    default_cfg = os.path.join(os.path.dirname(__file__), "config.yaml")
    parser.add_argument("--config", default=default_cfg)
    args = parser.parse_args()

    cfg = load_config(args.config)
    broker = os.environ.get("MQTT_BROKER", cfg["mqtt_broker"])
    port = int(os.environ.get("MQTT_PORT", cfg["mqtt_port"]))
    interval = float(os.environ.get("PUBLISH_INTERVAL", cfg["publish_interval_sec"]))
    anomaly_prob = float(os.environ.get("ANOMALY_PROBABILITY", cfg["anomaly_probability"]))

    client = mqtt.Client(client_id="smartfactory-sensor-simulator")
    client.connect(broker, port, keepalive=60)
    client.loop_start()

    print(f"[simulator] connected to {broker}:{port}, publishing every {interval}s "
          f"for devices: {[d['device_id'] for d in cfg['devices']]}")

    start = time.time()
    try:
        while True:
            t = time.time() - start
            for device in cfg["devices"]:
                device_id = device["device_id"]
                for sensor_name, sensor_cfg in cfg["sensors"].items():
                    inject = random.random() < anomaly_prob
                    reading = generate_reading(sensor_name, sensor_cfg, t, inject)
                    payload = {
                        "device_id": device_id,
                        "sensor_type": sensor_name,
                        "value": reading["value"],
                        "unit": reading["unit"],
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    topic = f"factory/{device_id}/{sensor_name}/raw"
                    client.publish(topic, json.dumps(payload), qos=0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[simulator] stopping.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
