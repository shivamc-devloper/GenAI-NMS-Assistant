from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from collector import get_devices
from services.librenms_device_details_api import get_device_details_from_api
from services.librenms_db import get_memory_usage, get_cpu_usage

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/collector")
def collector_device():
    data = get_devices()
    return data


@app.get("/device-details")
def get_device_detail(host):
    data = get_device_details_from_api(host=host)
    return data


@app.get("/memory-usage")
def get_device_memory_usage(host):
    data = get_memory_usage(device_id=host)
    return data


@app.get("/cpu-usage")
def get_device_memory_usage(host):
    data = get_cpu_usage(device_id=host)
    return data
