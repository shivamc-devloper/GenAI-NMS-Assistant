import asyncio
import logging
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import List, Dict, Any, Optional

from .utils.config import settings

logger = logging.getLogger("anomaly")
logger.setLevel(logging.INFO)

# Try import DB helpers
try:
    from . import store
    get_timeseries_window = store.get_timeseries_window
    insert_alert = store.insert_alert
    list_devices_snapshots = store.list_devices_snapshots
    DB_AVAILABLE = True
except Exception:
    logger.warning("db.store not available — anomaly runner using in-memory helpers")
    DB_AVAILABLE = False

    # Small in-memory helpers that match expected signatures.
    _MEM = {"metrics": {}, "alerts": []}

    async def get_timeseries_window(device_id: str, metric: str, window_seconds: int):
        # key stored by collector: (_device_id, metric) -> list of points with {"ts": iso, "value": v}
        from collector.ingest import _MEMORY  # type: ignore
        key = (device_id, metric)
        pts = _MEMORY["metrics"].get(key, [])
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        return [ {"ts": p["ts"], "value": p["value"]} for p in pts if datetime.fromisoformat(p["ts"]) >= cutoff ]

    async def insert_alert(alert: Dict[str, Any]):
        _MEM["alerts"].append(alert)
        logger.info("ALERT(inserted): %s", alert)

    async def list_devices_snapshots():
        try:
            from collector.ingest import _MEMORY  # type: ignore
            return [ {"device_id": k, **v} for k, v in _MEMORY["devices"].items() ]
        except Exception:
            return []

# DETECTORS

async def threshold_detector(device_id: str, metric: str, latest_value: float) -> Optional[Dict[str, Any]]:
    """
    Simple static threshold detector. Customize thresholds in config.py.
    """
    # Map metrics to thresholds
    thr_map = {
        "cpu": int(settings.CPU_THRESHOLD or 85),
        "latency": int(settings.LATENCY_THRESHOLD or 100),
        "packet_loss": 2.0,
        "port_errors": 10,
        "memory": 90,
    }
    thr = thr_map.get(metric)
    if thr is None:
        return None
    if latest_value >= thr:
        return {
            "device_id": device_id,
            "metric": metric,
            "value": latest_value,
            "detector": "threshold",
            "severity": "critical" if latest_value >= thr * 1.1 else "major",
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "evidence": {"threshold": thr}
        }
    return None

async def zscore_detector(device_id: str, metric: str, window_seconds: int = 600) -> Optional[Dict[str, Any]]:
    """
    Compute z-score across values in the window; if z > 3 (configurable) flag anomaly.
    """
    pts = await get_timeseries_window(device_id, metric, window_seconds)
    values = [float(p["value"]) for p in pts if p.get("value") is not None]
    if len(values) < 6:
        return None
    try:
        mu = mean(values)
        sigma = stdev(values)
        latest = values[-1]
        if sigma == 0:
            return None
        z = (latest - mu) / sigma
        if abs(z) >= 3:
            return {
                "device_id": device_id,
                "metric": metric,
                "value": latest,
                "detector": "zscore",
                "zscore": z,
                "severity": "major" if abs(z) < 5 else "critical",
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "evidence": {"mean": mu, "stdev": sigma, "samples": len(values)}
            }
    except Exception:
        logger.exception("zscore detection error for %s/%s", device_id, metric)
    return None

# High-level runner

async def _check_device(device: Dict[str, Any]):
    """
    For a device snapshot, pull recent metrics and run detectors.
    """
    device_id = device.get("device_id")
    metrics_to_check = ["cpu", "latency", "port_errors", "memory"]

    for metric in metrics_to_check:
        try:
            pts = await get_timeseries_window(device_id, metric, window_seconds=600)
            if not pts:
                continue
            latest = float(pts[-1]["value"])
            # run threshold
            thr_alert = await threshold_detector(device_id, metric, latest)
            if thr_alert:
                await insert_alert(thr_alert)
                continue  # if threshold fired, skip other detectors for now
            # run z-score detector
            z_alert = await zscore_detector(device_id, metric, window_seconds=600)
            if z_alert:
                await insert_alert(z_alert)
        except Exception:
            logger.exception("error checking metric %s for device %s", metric, device_id)

async def run_anomaly_engine():
    """
    Background task to run anomaly detection continuously.
    The loop interval is intentionally shorter than ingestion to ensure timely detection.
    """
    interval = max(5, int(getattr(settings, "ANOMALY_POLL_INTERVAL", 15)))
    logger.info("Starting anomaly engine, interval=%s seconds", interval)
    while True:
        try:
            devices = await list_devices_snapshots()
            if not devices:
                logger.debug("No devices in snapshot to check")
            for d in devices:
                await _check_device(d)
        except Exception:
            logger.exception("anomaly engine iteration failed")
        await asyncio.sleep(interval)
