import openmeteo_requests
import requests_cache
import pandas as pd
from pandas import DataFrame
from retry_requests import retry
from sqlalchemy import insert
from models import HourlyWeatherRecord
from constants import ENGINE, LATITUDE, LONGITUDE
from models import HourlyWeatherRecordInstance


def get_hourly_weather_records_by_date(start_date: str, end_date: str) -> DataFrame:
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/Denver",
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ),
        "temperature": hourly_temperature_2m,
        "precipitation": hourly_precipitation,
        "wind_speed": hourly_wind_speed_10m,
    }
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


def insert_hourly_weather_records(records: pd.DataFrame):
    for row in records.itertuples():
        hourly_weather_record: HourlyWeatherRecordInstance = (
            HourlyWeatherRecordInstance.from_array(list(row[1::1]))
        )
        stmt = insert(HourlyWeatherRecord).values(
            date=hourly_weather_record.date,
            temperature=hourly_weather_record.temperature,
            precipitation=hourly_weather_record.precipitation,
            wind_speed=hourly_weather_record.wind_speed,
        )
        with ENGINE.connect() as cursor:
            cursor.execute(stmt)
            cursor.commit()
