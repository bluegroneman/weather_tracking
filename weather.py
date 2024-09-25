import pdb

import typer
from models import HourlyWeatherRecord, DailyWeatherRecord, DailyWeatherRecordInstance
from sqlalchemy import select, Row, insert
from constants import ENGINE, LATITUDE, LONGITUDE
from datetime import datetime
from datetime import timedelta
import pandas as pd
from dotenv import load_dotenv
from weather_api_importer import (
    get_hourly_weather_records_by_date,
    insert_hourly_weather_records,
)

load_dotenv()

app = typer.Typer()


@app.command()
def get_latest_hourly() -> Row:
    stmt = select(HourlyWeatherRecord).order_by(HourlyWeatherRecord.id.desc())
    with ENGINE.connect() as conn:
        results = conn.execute(stmt)
    return results.fetchone()


def get_latest_daily() -> Row:
    stmt = select(DailyWeatherRecord).order_by(DailyWeatherRecord.id.desc())
    with ENGINE.connect() as conn:
        results = conn.execute(stmt)
    return results.fetchone()


@app.command()
def update_hourly():
    latest_record = get_latest_hourly()
    latest_date = datetime.strftime(latest_record.date, "%Y-%m-%d")
    today_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    print(f"Updating the hourly record table between {latest_date} - {today_date}")
    records = get_hourly_weather_records_by_date(latest_date, today_date)
    clean_records = records.dropna()
    insert_hourly_weather_records(clean_records)


@app.command()
def create_hourly_csv() -> None:
    stmt = select(HourlyWeatherRecord)
    with ENGINE.connect() as conn:
        results = conn.execute(stmt)
    hourly_weather_records = pd.DataFrame(results.fetchall())
    hourly_weather_records['date'] = pd.to_datetime(hourly_weather_records['date'])
    # Convert to RFC3339 date format for InfluxDB to import
    hourly_weather_records['date'] = hourly_weather_records['date'].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    file_name = "hourly_weather_records.csv"
    hourly_weather_records.to_csv(file_name, index=False)
    with open(file_name, 'r') as read_file:
        file_data = read_file.readlines()
    with open(file_name, 'w') as write_file:
        prepend_line = "#datatype lander_weather,long,dateTime:RFC3339,double,double,double\n"
        file_data.insert(0, prepend_line)
        write_file.writelines(file_data)


@app.command()
def update_daily():
    latest_record = get_latest_daily()
    latest_date = datetime.strftime(
        latest_record.date_time + timedelta(days=1), "%Y-%m-%d"
    )
    today_date = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
    if latest_date == today_date:
        raise ValueError("Latest date and today are the same")
    else:
        print(f"Updating the daily record table between {latest_date} - {today_date}")
        date_range = pd.date_range(start=latest_date, end=today_date)
        for date in date_range:
            hourly_rolled_up = HourlyWeatherRecord.get_weather_record_on_date(
                datetime.strftime(date, "%Y-%m-%d")
            )
            if hourly_rolled_up is not None:
                record: list = [
                    LATITUDE,
                    LONGITUDE,
                    date,
                    date.month,
                    date.day,
                    date.year,
                    hourly_rolled_up["temperature"].mean(),
                    hourly_rolled_up["temperature"].min(),
                    hourly_rolled_up["temperature"].max(),
                    hourly_rolled_up["wind_speed"].mean(),
                    hourly_rolled_up["wind_speed"].min(),
                    hourly_rolled_up["wind_speed"].max(),
                    hourly_rolled_up["precipitation"].sum(),
                    hourly_rolled_up["precipitation"].min(),
                    hourly_rolled_up["precipitation"].max(),
                ]
                daily_record: DailyWeatherRecordInstance = DailyWeatherRecordInstance(
                    *record
                )
                stmt = insert(DailyWeatherRecord).values(
                    latitude=daily_record.latitude,
                    longitude=daily_record.longitude,
                    date_time=daily_record.date_time,
                    month=daily_record.month,
                    day_of_month=daily_record.day_of_month,
                    year=daily_record.year,
                    average_temperature=daily_record.average_temperature,
                    min_temperature=daily_record.min_temperature,
                    max_temperature=daily_record.max_temperature,
                    average_wind_speed=daily_record.average_wind_speed,
                    min_wind_speed=daily_record.min_wind_speed,
                    max_wind_speed=daily_record.max_wind_speed,
                    precipitation_sum=daily_record.precipitation_sum,
                    precipitation_min=daily_record.precipitation_min,
                    precipitation_max=daily_record.precipitation_max,
                )
                with ENGINE.connect() as cursor:
                    cursor.execute(stmt)
                    cursor.commit()


if __name__ == "__main__":
    app()
