from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta, timezone
from collector import add_device_to_librenms
import logging
import store as db_store  # using the module created above
from utils.config import settings

import httpx
import asyncio
try:
    import asyncssh
    ASYNC_SSH_AVAILABLE = True
except Exception:
    ASYNC_SSH_AVAILABLE = False

from pydantic import BaseModel
from typing import Literal


router = APIRouter(prefix="/api", tags=["dashboard"])
logger = logging.getLogger("api.router")

class DeviceCreate(BaseModel):
    device_id: str = Field(..., description="Unique device id")
    hostname: Optional[str] = None
    snmp_community: Optional[str] = "public"
    snmp_version: Optional[str] = "v2c"

class SnmpControl(BaseModel):
    action: Literal["disable", "enable"] = "disable"
    method: Literal["librenms", "ssh", "db"] = "librenms"
    # only for ssh method
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None  # you may prefer keys; example kept simple
    ssh_port: Optional[int] = 22
    # for librenms method you may pass options (e.g. remove or set community "")
    librenms_remove: Optional[bool] = False
    # optional reason stored in DB/log
    reason: Optional[str] = None


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
        "last_seen": datetime.now(timezone.utc).isoformat(),
        
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
            "snmp_community": payload.snmp_community or "public",
            "snmp_version": payload.snmp_version or "v2c",
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

# Helper: call LibreNMS API to disable (or update) device
async def _librenms_disable_device(hostname: str, remove: bool = False):
    """
    Uses settings.LIBRENS_API_URL and settings.LIBRENS_API_TOKEN (configure these)
    If remove=True -> attempt to delete device from LibreNMS
    Otherwise -> update device to disable SNMP (clear SNMP fields or set disabled flag)
    """
    api_url = getattr(settings, "LIBRENS_API_URL", None)
    api_token = getattr(settings, "LIBRENS_API_TOKEN", None)
    if not api_url or not api_token:
        raise RuntimeError("LibreNMS API URL or token not configured in settings")

    headers = {"X-Auth-Token": api_token, "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        if remove:
            # try delete by hostname (LibreNMS expects ID or hostname endpoint depends on your version)
            # we'll try to delete by hostname
            resp = await client.delete(f"{api_url}/devices/{hostname}", headers=headers)
            if resp.status_code not in (200, 204):
                raise RuntimeError(f"LibreNMS delete failed: {resp.status_code} {resp.text}")
            return {"librenms": "deleted"}
        else:
            # patch/update device: clear SNMP community and optionally set disabled flag
            # Exact LibreNMS fields vary by version; typical field names: 'snmp_community', 'snmp_version', 'disabled'
            payload = {"snmp_community": "", "snmp_version": "", "disabled": 1}
            resp = await client.patch(f"{api_url}/devices/{hostname}", headers=headers, json=payload)
            if resp.status_code not in (200, 202):
                # Some LibreNMS versions may not accept PATCH on /devices/{hostname}; try POST to /devices/edit
                # Fallback: try POST to /devices with 'disabled' flag (best-effort; adjust for your LibreNMS)
                raise RuntimeError(f"LibreNMS update failed: {resp.status_code} {resp.text}")
            return {"librenms": "disabled"}
        
# Helper: run SSH commands to stop and disable snmpd on remote host
async def _ssh_disable_snmp(host: str, user: str, password: Optional[str], port: int = 22):
    """
    Attempts async SSH using asyncssh. If asyncssh isn't available, raise helpful error.
    Command executed: sudo systemctl stop snmpd && sudo systemctl disable snmpd
    NOTE: remote host must allow password or key auth and user must be able to run sudo without interactive prompt.
    """
    if not ASYNC_SSH_AVAILABLE:
        raise RuntimeError("asyncssh not available on the API server. Install asyncssh for SSH operations.")
    cmd = "sudo systemctl stop snmpd && sudo systemctl disable snmpd || true"
    try:
        conn = await asyncssh.connect(host, port=port, username=user, password=password, known_hosts=None)
        result = await conn.run(cmd, check=False)
        await conn.close()
        return {"stdout": result.stdout, "stderr": result.stderr, "exit_status": result.exit_status}
    except Exception as e:
        raise RuntimeError(f"SSH error: {e}")

# Main API endpoint to enable/disable SNMP for a device
@router.post("/devices/{device_id}/snmp")
async def control_snmp(device_id: str, payload: SnmpControl = Body(...)):
    """
    Control SNMP for a device.
    - method=librenms: uses LibreNMS API to disable monitoring (recommended for monitoring-only disable)
    - method=ssh: runs systemctl stop/disable on the remote host over SSH (actually stops service)
    - method=db: flips a local snmp_enabled flag in DB and optionally remove from LibreNMS
    """
    # 1) resolve device info from your DB to find hostname / ip
    device = await db_store.get_device(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    hostname = device.get("hostname") or device.get("device_id") or device.get("management_ip")
    ip = device.get("management_ip") or hostname

    try:
        if payload.method == "librenms":
            # call LibreNMS API
            res = await _librenms_disable_device(hostname, remove=payload.librenms_remove)
            # store change locally
            await db_store.update_device_snapshot(device_id, {**device, "snmp_enabled": (payload.action == "enable")})
            return {"status": "ok", "method": "librenms", "result": res}

        elif payload.method == "ssh":
            if payload.action == "enable":
                # enabling via SSH: start & enable service
                cmd = "sudo systemctl start snmpd && sudo systemctl enable snmpd || true"
                if not ASYNC_SSH_AVAILABLE:
                    raise HTTPException(status_code=500, detail="asyncssh not installed on API server")
                conn = await asyncssh.connect(ip, port=payload.ssh_port or 22,
                                              username=payload.ssh_user, password=payload.ssh_password, known_hosts=None)
                result = await conn.run(cmd, check=False)
                await conn.close()
                await db_store.update_device_snapshot(device_id, {**device, "snmp_enabled": True})
                return {"status": "ok", "method": "ssh", "stdout": result.stdout, "stderr": result.stderr, "exit": result.exit_status}
            else:
                # disable via SSH
                res = await _ssh_disable_snmp(ip, payload.ssh_user, payload.ssh_password, payload.ssh_port or 22)
                await db_store.update_device_snapshot(device_id, {**device, "snmp_enabled": False})
                return {"status": "ok", "method": "ssh", "result": res}

        elif payload.method == "db":
            # only update local DB and optionally remove from LibreNMS if requested
            new_flag = (payload.action == "enable")
            await db_store.update_device_snapshot(device_id, {**device, "snmp_enabled": new_flag})
            res = {"db": "updated", "snmp_enabled": new_flag}
            if payload.librenms_remove and not new_flag:
                try:
                    ln_res = await _librenms_disable_device(hostname, remove=True)
                    res["librenms"] = ln_res
                except Exception as e:
                    res["librenms_error"] = str(e)
            return {"status": "ok", "method": "db", "result": res}

        else:
            raise HTTPException(status_code=400, detail="Unknown method")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("SNMP control failed for %s: %s", device_id, e)
        raise HTTPException(status_code=500, detail=str(e))


