from sqlalchemy import insert, select
from weather_api_importer import (
    get_hourly_weather_records_by_date,
    insert_hourly_weather_records,
)


from models import (
    Base,
    Location,
    HourlyWeatherRecord,
    DailyWeatherRecord,
)
import pandas as pd
import typer
from constants import ENGINE, START_DATE, END_DATE, LATITUDE, LONGITUDE
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

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
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt_inclusive = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD for start and end dates.")
        raise
    logger.info("Importing weather data...")
    existing_ts = set()
    with ENGINE.connect() as conn:
        stmt = (
            select(HourlyWeatherRecord.date)
            .where(HourlyWeatherRecord.date >= start_dt)
            .where(HourlyWeatherRecord.date < end_dt_inclusive)
        )
        result = conn.execute(stmt).scalars().all()
        # Normalize to naive datetimes for consistent comparison (drop tz if present)
        for dt in result:
            try:
                if getattr(dt, "tzinfo", None) is not None:
                    dt = dt.replace(tzinfo=None)
            except Exception:
                pass
            existing_ts.add(dt)

    if existing_ts:
        logger.info(
            f"Found {len(existing_ts)} existing hourly records in range {start_date}..{end_date}; will skip duplicates."
        )

    logger.info("Getting weather records from API")
    records = get_hourly_weather_records_by_date(start_date, end_date)

    # Filter out records that already exist
    if not records.empty and existing_ts:
        records = records.copy()
        # Ensure pandas Timestamp is naive (drop timezone) for comparison with DB values
        records["_date_naive"] = pd.to_datetime(records["date"]).dt.tz_localize(None)
        before = len(records)
        records = records[~records["_date_naive"].isin(existing_ts)].drop(
            columns=["_date_naive"]
        )
        after = len(records)
        skipped = before - after
        if skipped > 0:
            logger.info(f"Skipping {skipped} duplicate hourly rows already in DB.")

    if records is None or records.empty:
        logger.info("No new hourly weather records to insert.")
        return

    insert_hourly_weather_records(records)

@app.command()
def build_daily_summaries():
    """Build daily_weather from ALL data in hourly_weather using pandas.
    """
    DailyWeatherRecord.__table__.drop(ENGINE, checkfirst=True)
    DailyWeatherRecord.__table__.create(ENGINE, checkfirst=True)

    logger.info("Building daily summaries from all hourly data...")

    # Load all hourly data into a DataFrame
    with ENGINE.connect() as conn:
        hourly_df = pd.read_sql(
            "SELECT date, temperature, precipitation, wind_speed FROM hourly_weather",
            conn,
        )

    if hourly_df is None or hourly_df.empty:
        logger.info("No hourly_weather data found; nothing to summarize.")
        return

    # Ensure proper dtypes and derive the day bucket
    hourly_df["date"] = pd.to_datetime(hourly_df["date"], errors="coerce")
    hourly_df["day"] = hourly_df["date"].dt.normalize()

    # Group by day and compute aggregates via pandas
    agg_df = (
        hourly_df.groupby("day").agg(
            average_temperature=("temperature", "mean"),
            min_temperature=("temperature", "min"),
            max_temperature=("temperature", "max"),
            average_wind_speed=("wind_speed", "mean"),
            min_wind_speed=("wind_speed", "min"),
            max_wind_speed=("wind_speed", "max"),
            precipitation_sum=("precipitation", "sum"),
            precipitation_min=("precipitation", "min"),
            precipitation_max=("precipitation", "max"),
        )
        .reset_index()
        .rename(columns={"day": "date_time"})
    )

    if agg_df.empty:
        logger.info("Aggregation produced no rows; nothing to insert.")
        return

    # Add calendar columns and location_id
    agg_df["month"] = agg_df["date_time"].dt.month.astype(int)
    agg_df["day_of_month"] = agg_df["date_time"].dt.day.astype(int)
    agg_df["year"] = agg_df["date_time"].dt.year.astype(int)
    agg_df["location_id"] = 1

    # Reorder columns to match the model
    agg_df = agg_df[
        [
            "location_id",
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
    ]

    records = agg_df.to_dict(orient="records")
    stmt = insert(DailyWeatherRecord)
    with ENGINE.begin() as conn:
        for record in records:
            conn.execute(stmt, record)

        conn.commit()
        logger.info(f"Inserted {len(records)} daily summary rows.")


if __name__ == "__main__":
    app()
