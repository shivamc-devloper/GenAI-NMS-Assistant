import requests
from utils.config import API, HEADERS


def get_device_details_from_api(host):
    url = f"{API}/devices/{host}"
    return requests.get(url, headers=HEADERS).json()
