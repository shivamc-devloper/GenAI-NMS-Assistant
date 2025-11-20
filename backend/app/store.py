# from __future__ import annotations

# from typing import List, Dict, Any, Optional
# from datetime import datetime, timedelta, timezone

# import logging

# from sqlalchemy import (
#     String,
#     Integer,
#     Float,
#     DateTime,
#     func,
#     select,
#     ForeignKey,
# )
# from sqlalchemy.dialects.postgresql import JSONB
# from sqlalchemy.ext.asyncio import (
#     create_async_engine,
#     async_sessionmaker,
#     AsyncSession,
# )
# from sqlalchemy.orm import declarative_base, Mapped, mapped_column

# from utils.config import settings

# logger = logging.getLogger("db.store")
# logger.setLevel(logging.INFO)

# Base = declarative_base()

# # ------------- Models -------------


# class Device(Base):
#     __tablename__ = "devices"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
#     device_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
#     hostname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
#     ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
#     last_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
#     raw: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)


# class Metric(Base):
#     __tablename__ = "metrics"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
#     device_id: Mapped[str] = mapped_column(String(255), index=True)
#     metric: Mapped[str] = mapped_column(String(64), index=True)
#     value: Mapped[float] = mapped_column(Float)
#     ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
#     tags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)


# class Alert(Base):
#     __tablename__ = "alerts"

#     id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
#     device_id: Mapped[str] = mapped_column(String(255), index=True)
#     metric: Mapped[str] = mapped_column(String(64))
#     value: Mapped[float] = mapped_column(Float)
#     detector: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
#     severity: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
#     detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
#     evidence: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)


# # ------------- Engine & Session -------------

# _engine = None
# SessionLocal: async_sessionmaker[AsyncSession] | None = None


# async def init_db():
#     """
#     Initialize PostgreSQL engine and create tables.
#     Call once at app startup (main.py).
#     """
#     global _engine, SessionLocal

#     # Example: postgresql+asyncpg://user:pass@localhost:5432/dbname
#     db_url = settings.DB_URL
#     logger.info("Connecting to PostgreSQL: %s", db_url)

#     _engine = create_async_engine(db_url, echo=False, future=True)
#     SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

#     # Create tables
#     async with _engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)

#     logger.info("PostgreSQL initialized and tables created")


# # ------------- Helper: session context -------------


# def _get_session() -> async_sessionmaker[AsyncSession]:
#     if SessionLocal is None:
#         raise RuntimeError("DB not initialized. Call init_db() first.")
#     return SessionLocal


# # ------------- Device helpers -------------


# async def update_device_snapshot(device_id: str, snapshot: Dict[str, Any]):
#     """
#     Upsert a device snapshot.
#     """
#     Session = _get_session()
#     async with Session() as session:
#         # Normalize last_seen
#         last_seen = snapshot.get("last_seen")
#         if isinstance(last_seen, str):
#             try:
#                 last_seen = datetime.fromisoformat(last_seen)
#             except Exception:
#                 last_seen = datetime.now(timezone.utc)
#         elif last_seen is None:
#             last_seen = datetime.now(timezone.utc)

#         stmt = select(Device).where(Device.device_id == device_id)
#         result = await session.execute(stmt)
#         dev = result.scalar_one_or_none()

#         if dev is None:
#             dev = Device(
#                 device_id=device_id,
#                 hostname=snapshot.get("hostname"),
#                 ip=snapshot.get("ip"),
#                 last_seen=last_seen,
#                 raw=snapshot.get("raw"),
#             )
#             session.add(dev)
#         else:
#             dev.hostname = snapshot.get("hostname", dev.hostname)
#             dev.ip = snapshot.get("ip", dev.ip)
#             dev.last_seen = last_seen
#             dev.raw = snapshot.get("raw", dev.raw)

#         await session.commit()


# async def list_devices_from_store() -> List[Dict[str, Any]]:
#     """
#     Return device snapshots as list of dicts.
#     """
#     Session = _get_session()
#     async with Session() as session:
#         stmt = select(Device)
#         result = await session.execute(stmt)
#         devices = result.scalars().all()

#         out: List[Dict[str, Any]] = []
#         for d in devices:
#             out.append(
#                 {
#                     "device_id": d.device_id,
#                     "hostname": d.hostname,
#                     "ip": d.ip,
#                     "last_seen": d.last_seen.isoformat() if d.last_seen else None,
#                     "raw": d.raw,
#                 }
#             )
#         return out


# async def list_devices_snapshots() -> List[Dict[str, Any]]:
#     """
#     Alias used by anomaly runner.
#     """
#     return await list_devices_from_store()


# # ------------- Metrics helpers -------------


# async def insert_metric_point(
#     device_id: str,
#     metric_name: str,
#     value: float,
#     ts: datetime,
#     tags: Optional[Dict] = None,
# ):
#     Session = _get_session()
#     if isinstance(ts, str):
#         try:
#             ts = datetime.fromisoformat(ts)
#         except Exception:
#             ts = datetime.now(timezone.utc)

#     async with Session() as session:
#         m = Metric(
#             device_id=device_id,
#             metric=metric_name,
#             value=float(value),
#             ts=ts,
#             tags=tags or {},
#         )
#         session.add(m)
#         await session.commit()


# async def get_timeseries_window(
#     device_id: str,
#     metric: str,
#     window_seconds: int,
# ) -> List[Dict[str, Any]]:
#     Session = _get_session()
#     cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)

#     async with Session() as session:
#         stmt = (
#             select(Metric)
#             .where(
#                 Metric.device_id == device_id,
#                 Metric.metric == metric,
#                 Metric.ts >= cutoff,
#             )
#             .order_by(Metric.ts.asc())
#         )
#         result = await session.execute(stmt)
#         rows = result.scalars().all()

#         return [
#             {
#                 "ts": r.ts.isoformat(),
#                 "value": r.value,
#             }
#             for r in rows
#         ]


# async def get_latest_metric(device_id: str, metric: str) -> Optional[Dict[str, Any]]:
#     Session = _get_session()
#     async with Session() as session:
#         stmt = (
#             select(Metric)
#             .where(Metric.device_id == device_id, Metric.metric == metric)
#             .order_by(Metric.ts.desc())
#             .limit(1)
#         )
#         result = await session.execute(stmt)
#         row = result.scalar_one_or_none()
#         if row is None:
#             return None
#         return {"ts": row.ts.isoformat(), "value": row.value}


# async def query_timeseries_range(
#     device_id: str,
#     metric: str,
#     start_ts: datetime,
#     end_ts: datetime,
# ) -> List[Dict[str, Any]]:
#     Session = _get_session()
#     async with Session() as session:
#         stmt = (
#             select(Metric)
#             .where(
#                 Metric.device_id == device_id,
#                 Metric.metric == metric,
#                 Metric.ts >= start_ts,
#                 Metric.ts <= end_ts,
#             )
#             .order_by(Metric.ts.asc())
#         )
#         result = await session.execute(stmt)
#         rows = result.scalars().all()

#         return [
#             {
#                 "ts": r.ts.isoformat(),
#                 "value": r.value,
#             }
#             for r in rows
#         ]


# # ------------- Alerts helpers -------------


# async def insert_alert(alert: Dict[str, Any]):
#     """
#     Insert an alert row.
#     """
#     Session = _get_session()
#     detected_at = alert.get("detected_at")
#     if isinstance(detected_at, str):
#         try:
#             detected_at = datetime.fromisoformat(detected_at)
#         except Exception:
#             detected_at = datetime.now(timezone.utc)
#     elif detected_at is None:
#         detected_at = datetime.now(timezone.utc)

#     async with Session() as session:
#         a = Alert(
#             device_id=alert.get("device_id"),
#             metric=alert.get("metric"),
#             value=float(alert.get("value", 0)),
#             detector=alert.get("detector"),
#             severity=alert.get("severity"),
#             detected_at=detected_at,
#             evidence=alert.get("evidence") or {},
#         )
#         session.add(a)
#         await session.commit()
#         logger.info("Inserted alert for %s %s", a.device_id, a.metric)


# async def query_recent_alerts(limit: int = 50) -> List[Dict[str, Any]]:
#     Session = _get_session()
#     async with Session() as session:
#         stmt = (
#             select(Alert)
#             .order_by(Alert.detected_at.desc())
#             .limit(limit)
#         )
#         result = await session.execute(stmt)
#         rows = result.scalars().all()

#         out: List[Dict[str, Any]] = []
#         for r in rows:
#             out.append(
#                 {
#                     "device_id": r.device_id,
#                     "metric": r.metric,
#                     "value": r.value,
#                     "detector": r.detector,
#                     "severity": r.severity,
#                     "detected_at": r.detected_at.isoformat(),
#                     "evidence": r.evidence,
#                 }
#             )
#         return out


# # ------------- Aggregation helpers for API -------------


# async def count_devices_online(
#     online_window_seconds: int = 300,
# ) -> Dict[str, int]:
#     Session = _get_session()
#     now = datetime.now(timezone.utc)
#     cutoff = now - timedelta(seconds=online_window_seconds)

#     async with Session() as session:
#         total_stmt = select(func.count(Device.id))
#         online_stmt = select(func.count(Device.id)).where(Device.last_seen >= cutoff)

#         total_res = await session.execute(total_stmt)
#         online_res = await session.execute(online_stmt)

#         total = total_res.scalar_one() or 0
#         online = online_res.scalar_one() or 0

#         return {"total": total, "online": online}


# async def average_latency_overall(
#     window_seconds: int = 3600,
# ) -> Optional[float]:
#     Session = _get_session()
#     cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)

#     async with Session() as session:
#         stmt = (
#             select(func.avg(Metric.value))
#             .where(Metric.metric == "latency", Metric.ts >= cutoff)
#         )
#         result = await session.execute(stmt)
#         val = result.scalar_one_or_none()
#         if val is None:
#             return None
#         return float(val)


# async def top_devices_by_metric(
#     metric: str,
#     limit: int = 5,
#     window_seconds: int = 3600,
# ) -> List[Dict[str, Any]]:
#     Session = _get_session()
#     cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)

#     async with Session() as session:
#         stmt = (
#             select(Metric.device_id, func.avg(Metric.value).label("avg_val"))
#             .where(Metric.metric == metric, Metric.ts >= cutoff)
#             .group_by(Metric.device_id)
#             .order_by(func.avg(Metric.value).desc())
#             .limit(limit)
#         )
#         result = await session.execute(stmt)
#         rows = result.all()

#         return [
#             {"device_id": r.device_id, "value": float(r.avg_val)}
#             for r in rows
#         ]


from __future__ import annotations
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import asyncio

logger = logging.getLogger("db.store")
logger.setLevel(logging.INFO)

# ---------- runtime flag ----------
_DB_AVAILABLE = False
_USE_POSTGRES = False

# ---------- In-memory fallback store ----------
_MEM = {
    "devices": {},       # device_id -> snapshot dict
    "metrics": [],       # list of {device_id, metric, value, ts_iso, tags}
    "alerts": []         # list of alert dicts
}

# ---------- Postgres placeholders (populated if DB init succeeds) ----------
try:
    from sqlalchemy import select, func
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from sqlalchemy.orm import declarative_base, mapped_column, Mapped
    from sqlalchemy import Integer, String, Float, DateTime
    from sqlalchemy.dialects.postgresql import JSONB
    Base = declarative_base()
    # Models recreated below when DB is available
    SQLALCHEMY_AVAILABLE = True
except Exception:
    SQLALCHEMY_AVAILABLE = False

_engine = None
_SessionLocal = None

# ----------- Postgres ORM models (deferred) -----------
if SQLALCHEMY_AVAILABLE:
    class Device(Base):
        __tablename__ = "devices"
        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        device_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
        hostname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
        ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
        last_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
        raw: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    class Metric(Base):
        __tablename__ = "metrics"
        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        device_id: Mapped[str] = mapped_column(String(255), index=True)
        metric: Mapped[str] = mapped_column(String(64), index=True)
        value: Mapped[float] = mapped_column(Float)
        ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
        tags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    class Alert(Base):
        __tablename__ = "alerts"
        id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
        device_id: Mapped[str] = mapped_column(String(255), index=True)
        metric: Mapped[str] = mapped_column(String(64))
        value: Mapped[float] = mapped_column(Float)
        detector: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
        severity: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
        detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
        evidence: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)


# ----------------- Init function -----------------
async def init_db(db_url: Optional[str] = None):
    """
    Try to initialize Postgres. If it fails, keep using in-memory fallback.
    Call this at app startup.
    """
    global _DB_AVAILABLE, _USE_POSTGRES, _engine, _SessionLocal

    # allow optional param or read from env (import config lazily to avoid circular imports)
    try:
        from utils.config import settings
        db_url = db_url or settings.DB_URL
    except Exception:
        # fallback if settings unavailable
        db_url = db_url or None

    if not db_url:
        logger.warning("DB URL not provided -> using in-memory fallback")
        _DB_AVAILABLE = False
        _USE_POSTGRES = False
        return

    if not SQLALCHEMY_AVAILABLE:
        logger.warning("SQLAlchemy not available in environment -> using in-memory fallback")
        _DB_AVAILABLE = False
        _USE_POSTGRES = False
        return

    # Try connecting to Postgres
    try:
        _engine = create_async_engine(db_url, echo=False, future=True)
        _SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)
        # create tables if not exist
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        _DB_AVAILABLE = True
        _USE_POSTGRES = True
        logger.info("Connected to Postgres and initialized tables.")
    except Exception as e:
        logger.exception("Failed to initialize Postgres (%s). Falling back to in-memory store.", e)
        _DB_AVAILABLE = False
        _USE_POSTGRES = False

# ----------------- Helper to check mode -----------------
def is_db_available() -> bool:
    return _DB_AVAILABLE and _USE_POSTGRES

# ----------------- In-memory implementations -----------------
def _mem_insert_metric_point(device_id: str, metric_name: str, value: float, ts: datetime, tags: Optional[Dict]=None):
    ts_iso = ts.isoformat() if isinstance(ts, datetime) else str(ts)
    _MEM["metrics"].append({
        "device_id": device_id,
        "metric": metric_name,
        "value": float(value),
        "ts": ts_iso,
        "tags": tags or {}
    })

def _mem_update_device_snapshot(device_id: str, snapshot: Dict[str, Any]):
    s = snapshot.copy()
    # store last_seen as iso string
    if isinstance(s.get("last_seen"), datetime):
        s["last_seen"] = s["last_seen"].isoformat()
    _MEM["devices"][device_id] = s

def _mem_list_devices_from_store():
    out = []
    for device_id, snap in _MEM["devices"].items():
        rec = {"device_id": device_id}
        rec.update(snap)
        out.append(rec)
    return out

def _mem_get_timeseries_window(device_id: str, metric: str, window_seconds: int):
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    out = []
    for p in _MEM["metrics"]:
        if p["device_id"] == device_id and p["metric"] == metric:
            try:
                ts = datetime.fromisoformat(p["ts"])
            except Exception:
                continue
            if ts >= cutoff:
                out.append({"ts": p["ts"], "value": p["value"]})
    # sort by ts if necessary
    out.sort(key=lambda x: x["ts"])
    return out

def _mem_insert_alert(alert: Dict[str, Any]):
    if isinstance(alert.get("detected_at"), datetime):
        alert["detected_at"] = alert["detected_at"].isoformat()
    _MEM["alerts"].append(alert)
    logger.info("Inserted alert into in-memory store: %s", alert)

def _mem_query_recent_alerts(limit: int = 50):
    # sort by detected_at descending
    def key(a):
        try:
            return a.get("detected_at", "")
        except:
            return ""
    sorted_alerts = sorted(_MEM["alerts"], key=key, reverse=True)
    return sorted_alerts[:limit]

def _mem_query_timeseries_range(device_id: str, metric: str, start_ts: datetime, end_ts: datetime):
    out = []
    for p in _MEM["metrics"]:
        if p["device_id"] == device_id and p["metric"] == metric:
            try:
                ts = datetime.fromisoformat(p["ts"])
            except Exception:
                continue
            if start_ts <= ts <= end_ts:
                out.append({"ts": p["ts"], "value": p["value"]})
    out.sort(key=lambda x: x["ts"])
    return out

def _mem_count_devices_online(online_window_seconds: int = 300):
    total = len(_MEM["devices"])
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=online_window_seconds)
    online = 0
    for d in _MEM["devices"].values():
        last = d.get("last_seen")
        try:
            ts = datetime.fromisoformat(last) if isinstance(last, str) else last
        except Exception:
            ts = None
        if ts and isinstance(ts, datetime) and ts >= cutoff:
            online += 1
    return {"total": total, "online": online}

def _mem_average_latency_overall(window_seconds: int = 3600):
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    vals = []
    for p in _MEM["metrics"]:
        if p["metric"] == "latency":
            try:
                ts = datetime.fromisoformat(p["ts"])
            except Exception:
                continue
            if ts >= cutoff:
                vals.append(float(p["value"]))
    if not vals:
        return None
    return float(sum(vals) / len(vals))

def _mem_top_devices_by_metric(metric: str, limit: int = 5, window_seconds: int = 3600):
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    agg: Dict[str, List[float]] = {}
    for p in _MEM["metrics"]:
        if p["metric"] == metric:
            try:
                ts = datetime.fromisoformat(p["ts"])
            except Exception:
                continue
            if ts >= cutoff:
                agg.setdefault(p["device_id"], []).append(float(p["value"]))
    avg_list = [{"device_id": d, "value": sum(vs)/len(vs)} for d, vs in agg.items() if vs]
    avg_list.sort(key=lambda x: x["value"], reverse=True)
    return avg_list[:limit]

# --------------- Public API functions ---------------

# Device snapshot helpers
async def update_device_snapshot(device_id: str, snapshot: Dict[str, Any]):
    if is_db_available():
        Session = _SessionLocal
        async with Session() as session:
            stmt = select(Device).where(Device.device_id == device_id)
            res = await session.execute(stmt)
            dev = res.scalar_one_or_none()
            # normalize last_seen
            last_seen = snapshot.get("last_seen")
            if isinstance(last_seen, str):
                try:
                    last_seen = datetime.fromisoformat(last_seen)
                except Exception:
                    last_seen = datetime.now(timezone.utc)
            elif last_seen is None:
                last_seen = datetime.now(timezone.utc)
            if dev is None:
                dev = Device(device_id=device_id,
                             hostname=snapshot.get("hostname"),
                             ip=snapshot.get("ip"),
                             last_seen=last_seen,
                             raw=snapshot.get("raw"))
                session.add(dev)
            else:
                dev.hostname = snapshot.get("hostname", dev.hostname)
                dev.ip = snapshot.get("ip", dev.ip)
                dev.last_seen = last_seen
                dev.raw = snapshot.get("raw", dev.raw)
            await session.commit()
    else:
        # in-memory fallback
        _mem_update_device_snapshot(device_id, snapshot)

async def list_devices_from_store() -> List[Dict[str, Any]]:
    if is_db_available():
        Session = _SessionLocal
        async with Session() as session:
            stmt = select(Device)
            res = await session.execute(stmt)
            devices = res.scalars().all()
            out = []
            for d in devices:
                out.append({
                    "device_id": d.device_id,
                    "hostname": d.hostname,
                    "ip": d.ip,
                    "last_seen": d.last_seen.isoformat() if d.last_seen else None,
                    "raw": d.raw
                })
            return out
    else:
        return _mem_list_devices_from_store()

# Metrics helpers
async def insert_metric_point(device_id: str, metric_name: str, value: float, ts: datetime, tags: Optional[Dict]=None):
    if is_db_available():
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except Exception:
                ts = datetime.now(timezone.utc)
        async with _SessionLocal() as session:
            m = Metric(device_id=device_id, metric=metric_name, value=float(value), ts=ts, tags=tags or {})
            session.add(m)
            await session.commit()
    else:
        _mem_insert_metric_point(device_id, metric_name, value, ts, tags)

async def get_timeseries_window(device_id: str, metric: str, window_seconds: int) -> List[Dict[str, Any]]:
    if is_db_available():
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        async with _SessionLocal() as session:
            stmt = select(Metric).where(Metric.device_id == device_id, Metric.metric == metric, Metric.ts >= cutoff).order_by(Metric.ts.asc())
            res = await session.execute(stmt)
            rows = res.scalars().all()
            return [{"ts": r.ts.isoformat(), "value": r.value} for r in rows]
    else:
        return _mem_get_timeseries_window(device_id, metric, window_seconds)

async def get_latest_metric(device_id: str, metric: str) -> Optional[Dict[str, Any]]:
    if is_db_available():
        async with _SessionLocal() as session:
            stmt = select(Metric).where(Metric.device_id==device_id, Metric.metric==metric).order_by(Metric.ts.desc()).limit(1)
            res = await session.execute(stmt)
            r = res.scalar_one_or_none()
            if r is None:
                return None
            return {"ts": r.ts.isoformat(), "value": r.value}
    else:
        pts = _mem_get_timeseries_window(device_id, metric, 24*3600)
        return pts[-1] if pts else None

async def query_timeseries_range(device_id: str, metric: str, start_ts: datetime, end_ts: datetime) -> List[Dict[str, Any]]:
    if is_db_available():
        async with _SessionLocal() as session:
            stmt = select(Metric).where(Metric.device_id==device_id, Metric.metric==metric, Metric.ts>=start_ts, Metric.ts<=end_ts).order_by(Metric.ts.asc())
            res = await session.execute(stmt)
            rows = res.scalars().all()
            return [{"ts": r.ts.isoformat(), "value": r.value} for r in rows]
    else:
        return _mem_query_timeseries_range(device_id, metric, start_ts, end_ts)

# Alerts helpers
async def insert_alert(alert: Dict[str, Any]):
    if is_db_available():
        detected_at = alert.get("detected_at")
        if isinstance(detected_at, str):
            try:
                detected_at = datetime.fromisoformat(detected_at)
            except Exception:
                detected_at = datetime.now(timezone.utc)
        elif detected_at is None:
            detected_at = datetime.now(timezone.utc)
        async with _SessionLocal() as session:
            a = Alert(device_id=alert.get("device_id"),
                      metric=alert.get("metric"),
                      value=float(alert.get("value",0)),
                      detector=alert.get("detector"),
                      severity=alert.get("severity"),
                      detected_at=detected_at,
                      evidence=alert.get("evidence") or {})
            session.add(a)
            await session.commit()
    else:
        _mem_insert_alert(alert)

async def query_recent_alerts(limit: int = 50) -> List[Dict[str, Any]]:
    if is_db_available():
        async with _SessionLocal() as session:
            stmt = select(Alert).order_by(Alert.detected_at.desc()).limit(limit)
            res = await session.execute(stmt)
            rows = res.scalars().all()
            out = []
            for r in rows:
                out.append({"device_id": r.device_id, "metric": r.metric, "value": r.value, "detector": r.detector, "severity": r.severity, "detected_at": r.detected_at.isoformat(), "evidence": r.evidence})
            return out
    else:
        return _mem_query_recent_alerts(limit)

# Aggregation helpers
async def count_devices_online(online_window_seconds: int = 300) -> Dict[str, int]:
    if is_db_available():
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=online_window_seconds)
        async with _SessionLocal() as session:
            total = (await session.execute(select(func.count(Device.id)))).scalar_one() or 0
            online = (await session.execute(select(func.count(Device.id)).where(Device.last_seen >= cutoff))).scalar_one() or 0
            return {"total": total, "online": online}
    else:
        return _mem_count_devices_online(online_window_seconds)

async def average_latency_overall(window_seconds: int = 3600) -> Optional[float]:
    if is_db_available():
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        async with _SessionLocal() as session:
            val = (await session.execute(select(func.avg(Metric.value)).where(Metric.metric == "latency", Metric.ts >= cutoff))).scalar_one_or_none()
            if val is None:
                return None
            return float(val)
    else:
        return _mem_average_latency_overall(window_seconds)

async def top_devices_by_metric(metric: str, limit: int = 5, window_seconds: int = 3600) -> List[Dict[str, Any]]:
    if is_db_available():
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        async with _SessionLocal() as session:
            stmt = select(Metric.device_id, func.avg(Metric.value).label("avg_val")).where(Metric.metric == metric, Metric.ts >= cutoff).group_by(Metric.device_id).order_by(func.avg(Metric.value).desc()).limit(limit)
            res = await session.execute(stmt)
            rows = res.all()
            return [{"device_id": r.device_id, "value": float(r.avg_val)} for r in rows]
    else:
        return _mem_top_devices_by_metric(metric, limit, window_seconds)
