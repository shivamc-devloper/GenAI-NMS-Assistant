# import requests

# API = "http://localhost:8000/api/v0"
# TOKEN = "8db757baccab3961a2a9ecf52fac74ff"
# HEADERS = {"X-Auth-Token": TOKEN}


# def get_devices():
#     url = f"{API}/devices"
#     return requests.get(url=url, headers=HEADERS).json()


# def get_device_metrics(host):
#     cpu = requests.get(f"{API}/devices/{host}/health/cpu", headers=HEADERS).json
#     mem = requests.get(f"{API}/devices/{host}/health/memory", headers=HEADERS).json()
#     ports = requests.get(f"{API}/devices/{host}/ports", headers=HEADERS).json()
#     latency = requests.get(f"{API}/devices/{host}/latency", headers=HEADERS).json()

#     return {"cpu": cpu, "memory": mem, "ports": ports}



import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx

from utils.config import settings

logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)

# Try to import DB store layer (to be implemented in app/db/store.py)
try:
    from store import (
        insert_metric_point,
        update_device_snapshot,
        list_devices_from_store,
    )
    DB_AVAILABLE = True
except Exception:
    logger.warning("db.store not available — using in-memory fallback store")
    DB_AVAILABLE = False

    # Simple in-memory store fallback (not persistent, only for dev/prototype)
    _MEMORY = {"devices": {}, "metrics": {}, "alerts": {}}

    async def insert_metric_point(device_id: str, metric_name: str, value: float, ts: datetime, tags: Optional[Dict]=None):
        tags = tags or {}
        key = (device_id, metric_name)
        _MEMORY["metrics"].setdefault(key, []).append({"ts": ts.isoformat(), "value": value, "tags": tags})

    async def update_device_snapshot(device_id: str, snapshot: Dict[str, Any]):
        _MEMORY["devices"][device_id] = snapshot

    async def list_devices_from_store():
        # returns list of device dicts (id, hostname/ip) from memory
        return [{"device_id": k, **v} for k, v in _MEMORY["devices"].items()]

# HTTP client setup
CLIENT_TIMEOUT = 20.0
_headers = {"X-Auth-Token": settings.LIBRENMS_TOKEN} if settings.LIBRENMS_TOKEN else {}

async def _get_json(client: httpx.AsyncClient, path: str) -> Any:
    url = settings.LIBRENMS_URL.rstrip("/") + "/" + path.lstrip("/")
    try:
        r = await client.get(url, headers=_headers, timeout=CLIENT_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.exception("HTTP error fetching %s: %s", url, e)
        return None

async def fetch_devices() -> List[Dict[str, Any]]:
    """
    Fetch device list from LibreNMS. Expected return: list of device dicts
    """
    async with httpx.AsyncClient() as client:
        data = await _get_json(client, "api/v0/devices")
        if not data:
            return []
        # LibreNMS returns top-level object; adjust based on your LibreNMS version
        devices = data.get("devices") if isinstance(data, dict) and "devices" in data else data
        # Normalize device structure: {id, hostname, sysName, ip}
        normalized = []
        for d in devices:
            # try common fields
            device_id = str(d.get("device_id") or d.get("id") or d.get("hostname"))
            normalized.append({
                "device_id": device_id,
                "hostname": d.get("hostname") or d.get("sysName") or d.get("device_id"),
                "ip": d.get("ip") or d.get("ipv4"),
                "raw": d
            })
        return normalized

async def fetch_device_metrics(device: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch a small set of health metrics for the device.
    Endpoints used (per LibreNMS example):
    Adjust if your LibreNMS returns different paths.
    """
    async with httpx.AsyncClient() as client:
        device_key = device.get("hostname") or device.get("device_id")
        # try by device id or hostname depending on your LibreNMS
        base = f"api/v0/devices/{device_key}"
        cpu = await _get_json(client, f"{base}/health/cpu")
        mem = await _get_json(client, f"{base}/health/memory")
        ports = await _get_json(client, f"{base}/ports")
        latency = await _get_json(client, f"{base}/latency")
        return {
            "cpu": cpu,
            "memory": mem,
            "ports": ports,
            "latency": latency,
        }

def _normalize_points(device: Dict[str, Any], metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert LibreNMS responses to a canonical list of metric points:
      [{"device_id": "x", "metric": "cpu", "value": 12.3, "ts": datetime, "tags": {...}}, ...]
    The exact extraction depends on LibreNMS response payloads. This function contains sensible defaults.
    """
    points = []
    device_id = device.get("device_id") or device.get("hostname")
    ts = datetime.now(timezone.utc)
    # CPU
    try:
        cpu_val = None
        if isinstance(metrics.get("cpu"), dict):
            # might be { 'cpu': 12 } or similar
            # inspect common fields
            cpu_val = metrics["cpu"].get("cpu") or metrics["cpu"].get("usage") or metrics["cpu"].get("value")
        elif isinstance(metrics.get("cpu"), (int, float, str)):
            cpu_val = float(metrics["cpu"])
        if cpu_val is not None:
            points.append({"device_id": device_id, "metric": "cpu", "value": float(cpu_val), "ts": ts, "tags": {}})
    except Exception:
        logger.exception("failed to parse cpu metric for %s", device_id)

    # Memory
    try:
        mem = metrics.get("memory")
        mem_val = None
        if isinstance(mem, dict):
            mem_val = mem.get("used_percent") or mem.get("usage") or mem.get("memory")
        elif isinstance(mem, (int, float, str)):
            mem_val = float(mem)
        if mem_val is not None:
            points.append({"device_id": device_id, "metric": "memory", "value": float(mem_val), "ts": ts, "tags": {}})
    except Exception:
        logger.exception("failed to parse memory for %s", device_id)

    # Latency (if present, may be a list)
    try:
        lat = metrics.get("latency")
        if lat:
            if isinstance(lat, dict):
                # choose avg or min/max
                avg = lat.get("avg") or lat.get("average") or lat.get("value")
                if avg is not None:
                    points.append({"device_id": device_id, "metric": "latency", "value": float(avg), "ts": ts, "tags": {}})
            elif isinstance(lat, (int, float, str)):
                points.append({"device_id": device_id, "metric": "latency", "value": float(lat), "ts": ts, "tags": {}})
    except Exception:
        logger.exception("failed to parse latency for %s", device_id)

    # Ports: build a simple aggregate: errors or top util
    try:
        ports = metrics.get("ports") or []
        # If ports is dict with 'ports' key, normalize
        if isinstance(ports, dict) and "ports" in ports:
            ports = ports["ports"]
        # compute total if possible
        errors = 0
        for p in ports or []:
            if isinstance(p, dict):
                errors += int(p.get("ifInErrors", 0) or 0) + int(p.get("ifOutErrors", 0) or 0)
        points.append({"device_id": device_id, "metric": "port_errors", "value": float(errors), "ts": ts, "tags": {}})
    except Exception:
        logger.exception("failed to parse ports for %s", device_id)

    return points

async def ingest_once():
    """
    Single ingestion run:
     - fetch devices from LibreNMS
     - fetch per-device metrics
     - normalize and persist points
     - update snapshot
    """
    devices = await fetch_devices()
    if not devices:
        logger.warning("No devices fetched from LibreNMS")
        return

    for d in devices:
        try:
            metrics = await fetch_device_metrics(d)
            points = _normalize_points(d, metrics)
            # persist points
            for p in points:
                await insert_metric_point(p["device_id"], p["metric"], p["value"], p["ts"], tags=p.get("tags"))
            # update device snapshot (simple example)
            snapshot = {
                "device_id": d.get("device_id"),
                "hostname": d.get("hostname"),
                "ip": d.get("ip"),
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "raw": d.get("raw"),
            }
            await update_device_snapshot(d.get("device_id") or d.get("hostname"), snapshot)
        except Exception:
            logger.exception("failed to ingest metrics for device %s", d)

async def start_ingestion_loop():
    """
    Background loop entrypoint. Call this via asyncio.create_task(start_ingestion_loop()) from main.
    """
    interval = int(settings.INGEST_INTERVAL or 60)
    logger.info("Starting ingestion loop, interval=%s seconds", interval)
    while True:
        try:
            await ingest_once()
        except Exception:
            logger.exception("ingestion iteration failed")
        await asyncio.sleep(interval)

