from models import DailyWeatherRecord, HourlyWeatherRecord, Location
from sqlalchemy import select
import pandas as pd
from pandas import Series
from pytest import raises
from constants import ENGINE, LATITUDE, LONGITUDE


def test_is_unique():
    stmt = select(DailyWeatherRecord)
    with ENGINE.connect() as cursor:
        weather_df = pd.DataFrame(cursor.execute(stmt))
        clean_weather_df = weather_df.drop_duplicates()
        assert len(weather_df.count()) == len(clean_weather_df.count())


def test_has_correct_columns():
    stmt = select(DailyWeatherRecord)
    with ENGINE.connect() as conn:
        weather_df = pd.DataFrame(conn.execute(stmt))
        expected_column_names = [
            "id",
            "latitude",
            "longitude",
            "date_time",
            "month",
            "day_of_month",
            "year",
            "average_temperature",
            "min_temperature",
            "max_temperature",
            "average_wind_speed",
            "min_wind_speed",
            "max_wind_speed",
            "precipitation_sum",
            "precipitation_min",
            "precipitation_max",
        ]
        actual_column_names = weather_df.columns.values
        assert expected_column_names == actual_column_names.tolist()


def test_get_weather_record_by_date() -> None:
    record = DailyWeatherRecord.get_weather_record_on_date(date="2024-01-01")
    assert record is not None
    assert type(record["average_temperature"]) is Series
    assert type(record["min_temperature"]) is Series
    assert type(record["max_temperature"]) is Series
    assert type(record["average_wind_speed"]) is Series
    assert type(record["max_wind_speed"]) is Series
    assert type(record["min_wind_speed"]) is Series
    assert type(record["precipitation_sum"]) is Series
    assert type(record["precipitation_max"]) is Series
    assert type(record["precipitation_min"]) is Series


def test_invalid_date_format() -> None:
    try:
        DailyWeatherRecord.get_weather_record_on_date(date="2024-01-42")
    except ValueError:
        assert raises(ValueError)


def test_get_hourly_records_by_date() -> None:
    record = HourlyWeatherRecord.get_weather_record_on_date("2024-01-01")
    assert len(record) == 24, "Should be 24 hours in each records"


def test_get_location_data() -> None:
    stmt = select(Location)
    with ENGINE.connect() as cursor:
        record_df = pd.DataFrame(cursor.execute(stmt))

    assert len(record_df) == 1, "There should only be one location record"
    assert record_df["friendly_name"][0] == "Lander, Wyoming"
    assert float(record_df["latitude"][0]) == float(LATITUDE)
    assert float(record_df["longitude"][0]) == float(LONGITUDE)
