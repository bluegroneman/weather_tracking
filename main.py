from sqlalchemy import insert
from weather_api_importer import (
    get_hourly_weather_records_by_date,
    insert_hourly_weather_records,
)


from models import Base, Location, HourlyWeatherRecord, DailyWeatherRecord, DailyWeatherRecordInstance
import pandas as pd
import argparse
from constants import ENGINE, START_DATE, END_DATE, LATITUDE, LONGITUDE


def main():
    parser = argparse.ArgumentParser(
        prog="Weather Data",
        description="Command line arguments for weather data retrieval",
        epilog="Migration and data retrieval arguments for the weather database and api",
    )
    parser.add_argument(
        "-m", "--migrate", action="store_true", help="Should the database be migrated?"
    )
    parser.add_argument("-d", "--drop", action="store_true", help="Flag to drop tables")
    parser.add_argument(
        "-i",
        "--api-import",
        action="store_true",
        help="Flag to import data from the Open Meteo API",
    )
    parser.add_argument(
        "-b",
        "--build-daily",
        action="store_true",
        help="Flag to select daily weather data from hourly table",
    )
    args = parser.parse_args()
    if args.migrate:
        print("Creating new table schema...")
        Base.metadata.create_all(ENGINE)
        print("Creating location table data")
        stmt = insert(Location).values(
            latitude=LATITUDE, longitude=LONGITUDE, friendly_name="Lander, Wyoming"
        )

        with ENGINE.connect() as conn:
            conn.execute(stmt)
            conn.commit()
    if args.api_import:
        print("Getting weather records from API")
        records = get_hourly_weather_records_by_date(START_DATE, END_DATE)
        insert_hourly_weather_records(records)
    if args.build_daily:
        print("Building daily weather table")
        date_range = pd.date_range(start=START_DATE, end=END_DATE)
        for date in date_range:
            hourly_rolled_up = HourlyWeatherRecord.get_weather_record_on_date(
                f"{date.year}-{date.month}-{date.day}"
            )
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
    print("Done")


if __name__ == "__main__":
    main()
