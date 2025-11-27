import requests
from utils.config import API, HEADERS


def get_devices():
    url = f"{API}/devices"
    return requests.get(url=url, headers=HEADERS).json()


def get_device_metrics(host):
    cpu = requests.get(f"{API}/devices/{host}/health/cpu", headers=HEADERS).json
    mem = requests.get(f"{API}/devices/{host}/health/memory", headers=HEADERS).json()
    ports = requests.get(f"{API}/devices/{host}/ports", headers=HEADERS).json()
    return {"cpu": cpu, "memory": mem, "ports": ports}
