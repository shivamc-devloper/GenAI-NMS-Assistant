import requests

API = "http://localhost:8000/api/v0"
TOKEN = "8db757baccab3961a2a9ecf52fac74ff"
HEADERS = {"X-Auth-Token": TOKEN}

def device_nac():
    url = f"{API_BASE}/devices/6/graphs/device_processor"
    data = requests.get(url=url, headers=HEADERS).json()
    return data


print(device_nac())
