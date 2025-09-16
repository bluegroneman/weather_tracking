from sqlalchemy import insert, text, select
from weather_api_importer import (
    get_hourly_weather_records_by_date,
    insert_hourly_weather_records,
)


from models import (
    Base,
    Location,
    HourlyWeatherRecord,
    DailyWeatherRecord,
    DailyWeatherRecordInstance,
)
import pandas as pd
import typer
from constants import ENGINE, START_DATE, END_DATE, LATITUDE, LONGITUDE
import logging
from logging.handlers import RotatingFileHandler

# Configure logging to file with rotation
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = RotatingFileHandler(
    "weather_tracking.log", maxBytes=1_000_000, backupCount=3
)
_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
_handler.setFormatter(_formatter)
# Avoid adding multiple handlers if module is imported multiple times
if not logger.handlers:
    logger.addHandler(_handler)

app = typer.Typer(
    help="Weather Data CLI: migrate DB, import API data, and build daily summaries."
)


@app.command()
def migrate():
    logger.info("Creating new table schema...")
    Base.metadata.create_all(ENGINE)
    logger.info("Creating location table data")
    with ENGINE.begin() as conn:  # transactional context
        default_location = conn.execute(
            select(Location.id).where(
                Location.latitude == LATITUDE,
                Location.longitude == LONGITUDE,
            )
        ).scalar_one_or_none()

        if default_location is None:
            conn.execute(
                insert(Location).values(
                    latitude=LATITUDE,
                    longitude=LONGITUDE,
                    friendly_name="Lander, Wyoming",
                )
            )
            logger.info("Inserted seed location.")
        else:
            logger.info("Seed location already present; skipping insert.")


@app.command()
def import_weather_data(
    start_date: str = typer.Option(
        START_DATE, "-s", "--start-date", help="Start date format: YYYY-MM-DD"
    ),
    end_date: str = typer.Option(
        END_DATE, "-e", "--end-date", help="End date format: YYYY-MM-DD"
    ),
):
    # TODO: Check to ensure weather records between these dates aren't already in the DB
    # with ENGINE.begin() as conn:
        # Get oldest date
        # Get newest date
    logger.info("Importing weather data...")
    # TODO: implement check to ensure start and end date are in the correct format
    logger.info("Getting weather records from API")
    records = get_hourly_weather_records_by_date(start_date, end_date)
    insert_hourly_weather_records(records)


@app.command()
def build_daily_summaries(rebuild: bool = typer.Option(False)):
    if rebuild:
        DailyWeatherRecord.__table__.drop(ENGINE, checkfirst=True)
        DailyWeatherRecord.__table__.create(ENGINE, checkfirst=True)
    with ENGINE.begin() as conn:
        select(HourlyWeatherRecord).where(HourlyWeatherRecord.date >= START_DATE)
    logger.info("Building daily summaries...")
    date_range = pd.date_range(start=START_DATE, end=END_DATE)
    to_insert = []
    for date in date_range:
        # Format date once for query
        date_str = f"{date.year:04d}-{date.month:02d}-{date.day:02d}"
        hourly_rolled_up = HourlyWeatherRecord.get_weather_record_on_date(date_str)
        if hourly_rolled_up is None or hourly_rolled_up.empty:
            logger.warning(f"No hourly records found for {date_str}; skipping.")
            continue
        # Safely compute aggregates (convert NaN to None for DB)
        def _safe(value):
            try:
                import math
                return None if value is None or (isinstance(value, float) and math.isnan(value)) else value
            except Exception:
                return value
        payload = dict(
            location_id=1,
            date_time=date,
            month=int(date.month),
            day_of_month=int(date.day),
            year=int(date.year),
            average_temperature=_safe(hourly_rolled_up["temperature"].mean()),
            min_temperature=_safe(hourly_rolled_up["temperature"].min()),
            max_temperature=_safe(hourly_rolled_up["temperature"].max()),
            average_wind_speed=_safe(hourly_rolled_up["wind_speed"].mean()),
            min_wind_speed=_safe(hourly_rolled_up["wind_speed"].min()),
            max_wind_speed=_safe(hourly_rolled_up["wind_speed"].max()),
            precipitation_sum=_safe(hourly_rolled_up["precipitation"].sum()),
            precipitation_min=_safe(hourly_rolled_up["precipitation"].min()),
            precipitation_max=_safe(hourly_rolled_up["precipitation"].max()),
        )
        to_insert.append(payload)

    stmt = insert(DailyWeatherRecord)
    with ENGINE.connect() as cursor:
        cursor.execute(stmt, to_insert)
        cursor.commit()


if __name__ == "__main__":
    app()
