# from fastapi import FastAPI, Form
# from app.collector import get_devices

# app = FastAPI()


# @app.get("/collector")
# def collector_device():
#     data = get_devices()
#     return data

from fastapi import FastAPI
import asyncio
import logging
from utils.config import settings
from collector import start_ingestion_loop
from anomaly import run_anomaly_engine
import store as db_store
from router import router as api_router


app = FastAPI(
    title="AI Network Monitoring Backend",
    version="1.0.0",
    description="Telemetry ingestion, anomaly detection, RCA, and API"
)


# @app.on_event("startup")
# async def startup_event():
#     print("Starting backend services...")

#     # Initialize database connection
#     await init_db()

#     # Launch collector ingestion loop
#     asyncio.create_task(start_ingestion_loop())

#     # Launch anomaly detection runner
#     asyncio.create_task(run_anomaly_engine())

#     print(" System Initialized.")
@app.on_event("startup")
async def startup_event():
    logging.info("Starting backend services...")
    try:
        # init DB (db.store will fall bacclsk to memory if postgres unreachable)
        await db_store.init_db()
    except Exception as e:
        # init_db has internal try/except, but protect here too
        logging.exception("db.init_db() raised an unexpected exception (continuing with in-memory): %s", e)

    if db_store.is_db_available():
        logging.info("DB connected: using Postgres for persistence")
    else:
        logging.warning("DB not available: running with in-memory fallback (non-persistent)")

    # start background tasks (collector/anomaly). They will use db_store functions which work in both modes.
    asyncio.create_task(start_ingestion_loop())
    asyncio.create_task(run_anomaly_engine())
    logging.info("Startup complete.")


# API ROUTES
app.include_router(api_router)


# HEALTH

@app.get("/health", tags=["system"])
async def health_check():
    return {"status": "ok", "librenms": settings.LIBRENMS_URL}

