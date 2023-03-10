import datetime
import logging
import os
import traceback

from fastapi import FastAPI
import aiohttp
from dataclasses import dataclass
from dotenv import load_dotenv
import pandas as pd
from io import StringIO

load_dotenv()

app = FastAPI()
BASE_URL = os.getenv("INFLUX_URL")
TOKEN = os.getenv("INFLUX_TOKEN")
BUCKET = os.getenv("INFLUX_BUCKET")

log = logging.getLogger("uvicorn")
log.info(f"[SETUP] Serving interface to server {BASE_URL} with auth token {TOKEN} and default bucket {BUCKET}")


@dataclass
class PolynomialRegressionRequest:
    begin_date: datetime.datetime
    end_date: datetime.datetime
    frequency: str
    level: int


@dataclass
class FillNaRequest:
    method: str
    value: float


@dataclass
class ResampleRequest:
    frequency: str
    aggregate_method: str
    fillna: FillNaRequest


@dataclass
class IdsRequest:
    device_id: str
    service_profile_id: str


@dataclass
class DataRequest:
    end_date: datetime.datetime | None = None
    resample: ResampleRequest | None = None
    polynomial_regression: PolynomialRegressionRequest | None = None
    begin_date: datetime.datetime | None = None


def get_path(org: str) -> str:
    return f"{BASE_URL}/api/v2/query?org={org}"


def get_payload_query_since(bucket: str, since: int, device_id: str) -> str:
    return f'from(bucket:"{bucket}")|> range(start: -{since}h)|> filter(fn: (r) => r.device == "{device_id}")|>")'


def get_payload_query_4ever(bucket: str, device_id: str) -> str:
    return f'from(bucket:"{bucket}") |> range(start: 0) |> filter(fn: (r) => r.device == "{device_id}")'

    # return f'from(bucket:"{bucket}")|> range(start: v.timeRangeStart, stop: v.timeRangeStop)|> filter(fn: (r) => r.device == "{device_id}")|>)'


async def request_data(path: str, payload: dict | str) -> list | dict:
    async with aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/vnd.flux"

            }) as session:
        log.info(f"Request POST url={path} payload={payload}")
        async with session.post(url=path, data=payload) as resp:
            try:
                csv_string = await resp.text()
                csv_b = StringIO(csv_string)
                df = pd.read_csv(csv_b)
                log.info(f"Received DataFrame \n{df}\n")
                df = df.rename(columns={'_time': 'timestamp', '_value': 'value'})
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['timestamp'] = df['timestamp'] - pd.offsets.Hour(3)
                # timezone = 'America/Sao_Paulo'
                # df['timestamp'] = df['timestamp'].dt.convert(timezone)

                df['datetime'] = df['timestamp'].dt.strftime("%d/%m/%Y %H:%M")
                datetimesplit = df['datetime'].str.split(expand=True)
                df['date'] = datetimesplit[0]
                df['time'] = datetimesplit[1]
                data = df[['datetime', 'time', 'date', 'value', 'device']].to_dict(orient='records')
                dates = df['date'].unique().tolist()
                devices = df['device'].unique().tolist()
                result = {
                    'data': data,
                    'dates': dates,
                    'devices': devices
                }
                return result
            except Exception as e:
                log.exception(e)
                return []


@app.post("/organizations/{orgId}/export-sensor-data/devices/{deviceId}")
async def request_data_org(orgId: str, deviceId: str, data_request: DataRequest) -> dict:
    path = get_path(org=orgId)
    if data_request.begin_date:
        request_payload = get_payload_query_since(
            since=data_request.begin_date.hour,
            device_id=deviceId,
            bucket=BUCKET
        )
    else:
        request_payload = get_payload_query_4ever(
            bucket=BUCKET,
            device_id=deviceId
        )
    result = await request_data(path=path, payload=request_payload)
    return result
