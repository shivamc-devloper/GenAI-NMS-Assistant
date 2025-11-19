from fastapi import FastAPI, Form
from app.collector import get_devices

app = FastAPI()


@app.get("/collector")
def collector_device():
    data = get_devices()
    return data
