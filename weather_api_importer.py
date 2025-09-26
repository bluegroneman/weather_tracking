import openmeteo_requests
import requests_cache
import pandas as pd
from pandas import DataFrame
from retry_requests import retry
from sqlalchemy import insert
from models import HourlyWeatherRecord, OMSolarHourlyWeatherRecord
from constants import ENGINE, LATITUDE, LONGITUDE


def get_hourly_weather_records_by_date(
    start_date: str, end_date: str, lat: float = LATITUDE, long: float = LONGITUDE
) -> DataFrame:
    # Source: https://open-meteo.com/en/docs/historical-forecast-api?latitude=42.833&longitude=108.7307&timezone=America%2FDenver&start_date=2016-01-08&hourly=shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance,global_tilted_irradiance,terrestrial_radiation,soil_temperature_0cm,soil_temperature_54cm,soil_temperature_18cm,soil_temperature_6cm#settings
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": long,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "shortwave_radiation",
            "direct_radiation",
            "diffuse_radiation",
            "direct_normal_irradiance",
            "global_tilted_irradiance",
            "terrestrial_radiation",
        ],
        "timezone": "America/Denver",
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()
    hourly_shortwave_radiation = hourly.Variables(0).ValuesAsNumpy()
    hourly_direct_radiation = hourly.Variables(1).ValuesAsNumpy()
    hourly_diffuse_radiation = hourly.Variables(2).ValuesAsNumpy()
    hourly_direct_normal_irradiance = hourly.Variables(3).ValuesAsNumpy()
    hourly_global_tilted_irradiance = hourly.Variables(4).ValuesAsNumpy()
    hourly_terrestrial_radiation = hourly.Variables(5).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ),
        "shortwave_radiation": hourly_shortwave_radiation,
        "direct_radiation": hourly_direct_radiation,
        "diffuse_radiation": hourly_diffuse_radiation,
        "direct_normal_irradiance": hourly_direct_normal_irradiance,
        "global_tilted_irradiance": hourly_global_tilted_irradiance,
        "terrestrial_radiation": hourly_terrestrial_radiation,
    }

    return pd.DataFrame(data=hourly_data)


def insert_hourly_weather_records(records: pd.DataFrame):
    to_insert = []

    for row in records.itertuples(index=False):
        payload = {
            "location_id": 1,
            # Convert pandas Timestamp (possibly tz-aware) to naive python datetime
            "date": pd.to_datetime(getattr(row, "date")).to_pydatetime(),
            "shortwave_radiation": float(getattr(row, "shortwave_radiation")),
            "direct_radiation": float(getattr(row, "direct_radiation")),
            "diffuse_radiation": float(getattr(row, "diffuse_radiation")),
            "direct_normal_irradiance": float(getattr(row, "direct_normal_irradiance")),
            "global_tilted_irradiance": float(getattr(row, "global_tilted_irradiance")),
            "terrestrial_radiation": float(getattr(row, "terrestrial_radiation")),
        }
        to_insert.append(payload)

    if not to_insert:
        return

    stmt = insert(OMSolarHourlyWeatherRecord)
    # Execute in a transaction; executemany via list of dicts
    with ENGINE.begin() as conn:
        conn.execute(stmt, to_insert)
