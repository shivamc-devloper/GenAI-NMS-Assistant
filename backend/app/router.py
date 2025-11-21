from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta, timezone
from collector import add_device_to_librenms
import logging
import store as db_store  # using the module created above
from utils.config import settings

router = APIRouter(prefix="/api", tags=["dashboard"])
logger = logging.getLogger("api.router")

class DeviceCreate(BaseModel):
    device_id: str = Field(..., description="Unique device id")
    hostname: Optional[str] = None
    ip: Optional[str] = None
    snmp_community: Optional[str] = "public"
    model: Optional[str] = None
    vendor: Optional[str] = None
    notes: Optional[str] = None

# Health / summary
@router.get("/summary")
async def get_summary():
    """
    Returns:
    {
      "health_score": int,
      "major": int,
      "minor": int,
      "total_devices": int,
      "online": int,
      "average_latency": float|null
    }
    """
    counts = await db_store.count_devices_online()
    total = counts.get("total", 0)
    online = counts.get("online", 0)
    avg_latency = await db_store.average_latency_overall(window_seconds=3600)
    # basic health score: percent online; improved later using weighted issues
    health_score = int((online / total) * 100) if total > 0 else 100
    # quick counts of recent alerts => major/minor split (simple)
    alerts = await db_store.query_recent_alerts(limit=200)
    major = sum(1 for a in alerts if a.get("severity") in ("major", "critical"))
    minor = sum(1 for a in alerts if a.get("severity") in ("minor", "warning", None))
    return {
        "health_score": health_score,
        "major": major,
        "minor": minor,
        "total_devices": total,
        "online": online,
        "average_latency": avg_latency
    }

# Recommendations (stub: uses last alerts and simple rules)
@router.get("/recommendations")
async def get_recommendations(limit: int = 5):
    alerts = await db_store.query_recent_alerts(limit=50)
    recs = []
    seen = set()
    for a in alerts:
        metric = a.get("metric")
        device = a.get("device_id")
        if not device or not metric:
            continue
        key = f"{device}:{metric}"
        if key in seen:
            continue
        seen.add(key)
        if metric == "cpu":
            recs.append(f"Investigate high CPU usage on {device}")
        elif metric == "latency":
            recs.append(f"Check latency and interface stats for {device}")
        elif metric == "port_errors":
            recs.append(f"Inspect interfaces for errors on {device}")
        else:
            recs.append(f"Investigate {metric} on {device}")
        if len(recs) >= limit:
            break
    return {"recommendations": recs}

# Top devices
@router.get("/top-devices")
async def get_top_devices():
    top_bw = await db_store.top_devices_by_metric("bandwidth", limit=5, window_seconds=3600)
    top_cpu = await db_store.top_devices_by_metric("cpu", limit=5, window_seconds=3600)
    return {"top_bandwidth": top_bw, "top_cpu": top_cpu}

# Traffic/time series
@router.get("/traffic")
async def get_traffic(range_minutes: int = Query(60, ge=1, le=24*60), device_id: Optional[str] = None):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=range_minutes)
    if device_id:
        pts = await db_store.query_timeseries_range(device_id, "bandwidth", start, end)
    else:
        avg_list = await db_store.top_devices_by_metric("bandwidth", limit=10, window_seconds=range_minutes * 60)
        return {"timestamps": [], "data": avg_list}
    timestamps = [p["ts"] for p in pts]
    values = [p["value"] for p in pts]
    return {"timestamps": timestamps, "traffic": values}

# Alerts
@router.get("/alerts")
async def get_alerts(limit: int = 50):
    alerts = await db_store.query_recent_alerts(limit=limit)
    return alerts

# Device creation
@router.post("/devices")
async def create_device(payload: DeviceCreate = Body(...), add_to_librenms: bool = Query(False)):
    """
    Create a device locally and optionally add it to LibreNMS.
    """
    # 1) persist local snapshot
    snapshot = {
        "device_id": payload.device_id,
        "hostname": payload.hostname,
        "ip": payload.ip,
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "raw": {
            "model": payload.model,
            "vendor": payload.vendor,
            "notes": payload.notes
        }
    }

    try:
        await db_store.update_device_snapshot(payload.device_id, snapshot)
    except Exception as e:
        logger.exception("Failed to persist device snapshot: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to persist device snapshot: {e}")

    # 2) optionally add to LibreNMS using collector helper
    if add_to_librenms:
        ln_payload = {
            "hostname": payload.hostname or payload.device_id,
            "ip": payload.ip,
            "snmp_community": payload.snmp_community or "public",
            "os": payload.model or "generic",
            "model": payload.model,
            "notes": payload.notes or ""
        }
        try:
            ln_resp = await add_device_to_librenms(ln_payload)
            logger.info("LibreNMS add device response: %s", ln_resp)
        except Exception as e:
            logger.exception("LibreNMS create failed: %s", e)
            # decide policy: here we return 502 and keep local snapshot (you can change)
            raise HTTPException(status_code=502, detail=f"LibreNMS error: {e}")

    return {"status": "ok", "device": snapshot}


# Devices list
@router.get("/devices")
async def get_devices():
    devices = await db_store.list_devices_from_store()
    return devices

# Device timeseries
@router.get("/metrics/{device_id}")
async def get_device_metrics(device_id: str, metric: str = Query("cpu"), minutes: int = Query(60, ge=1)):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    pts = await db_store.query_timeseries_range(device_id, metric, start, end)
    return {"device_id": device_id, "metric": metric, "points": pts}

