import requests

API = "http://localhost:8000/api/v0"
TOKEN = "8db757baccab3961a2a9ecf52fac74ff"
HEADERS = {"X-Auth-Token": TOKEN}


def get_devices():
    url = f"{API}/devices"
    return requests.get(url=url, headers=HEADERS).json()


def get_device_metrics(host):
    cpu = requests.get(f"{API}/devices/{host}/health/cpu", headers=HEADERS).json
    mem = requests.get(f"{API}/devices/{host}/health/memory", headers=HEADERS).json()
    ports = requests.get(f"{API}/devices/{host}/ports", headers=HEADERS).json()
    return {"cpu": cpu, "memory": mem, "ports": ports}
