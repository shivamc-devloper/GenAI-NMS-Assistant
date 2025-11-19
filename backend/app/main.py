from fastapi import FastAPI, Form
from collector import get_devices

app = FastAPI()


@app.get("/collector")
def collector_device(host: str = Form(...)):
    data = get_devices()
    return data
