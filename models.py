from sqlalchemy import (
    String,
    Integer,
    Float,
    select,
    DateTime,
    create_engine,
    ForeignKey,
    Date,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta, date

from dataclasses import dataclass


class Base(DeclarativeBase):
    pass


class DailyWeatherRecord(Base):
    __tablename__ = "daily_weather"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("location.id"))
    date_time: Mapped[DateTime] = mapped_column(DateTime, unique=True)
    month: Mapped[int] = mapped_column(Integer)
    day_of_month: Mapped[int] = mapped_column(Integer)
    year: Mapped[int] = mapped_column(Integer)
    average_temperature: Mapped[float] = mapped_column(Float, nullable=True)
    min_temperature: Mapped[float] = mapped_column(Float, nullable=True)
    max_temperature: Mapped[float] = mapped_column(Float, nullable=True)
    average_wind_speed: Mapped[float] = mapped_column(Float, nullable=True)
    min_wind_speed: Mapped[Float] = mapped_column(Float, nullable=True)
    max_wind_speed: Mapped[float] = mapped_column(Float, nullable=True)
    precipitation_sum: Mapped[float] = mapped_column(Float, nullable=True)
    precipitation_min: Mapped[float] = mapped_column(Float, nullable=True)
    precipitation_max: Mapped[float] = mapped_column(Float, nullable=True)

    @classmethod
    def get_weather_record_on_date(cls, date: str) -> DataFrame:
        try:
            formatted_date = datetime.strptime(date, "%Y-%d-%m")
        except ValueError:
            raise ValueError("Invalid date, must be YYYY-MM-DD (%Y-%d-%m")
        stmt = select(cls).where(cls.date_time == formatted_date)
        engine = create_engine("sqlite:///weather.db")
        with engine.connect() as cursor:
            return pd.DataFrame(cursor.execute(stmt))

    @classmethod
    def get_mean_temperature_in_fahrenheit(cls, date: str) -> str:
        record = cls.get_weather_record_on_date(date)
        return f"The mean temperature on {date} is {record.iloc[0, 7]:.2f}° Fahrenheit."

    @classmethod
    def get_max_wind_speed_on_date(cls, date: str) -> str:
        record = cls.get_weather_record_on_date(date)
        return f"The max wind speed on {date} is {record.iloc[0, 12]:.2f} mph."

    @classmethod
    def get_precipitation_sum_on_date(cls, date):
        record = cls.get_weather_record_on_date(date)
        return (
            f"The total precipitation sum on {date} is {record.iloc[0, 13]:.2f} inches."
        )

    def __repr__(self) -> str:
        return f"Weather(id={self.id}"


class Location(Base):
    __tablename__ = "location"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    latitude: Mapped[String] = mapped_column(String)
    longitude: Mapped[String] = mapped_column(String)
    friendly_name: Mapped[String] = mapped_column(String)


class HourlyWeatherRecord(Base):
    __tablename__ = "hourly_weather"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("location.id"), default=1
    )
    date: Mapped[DateTime] = mapped_column(DateTime, unique=True)
    temperature: Mapped[float] = mapped_column(Float)
    precipitation: Mapped[float] = mapped_column(Float)
    wind_speed: Mapped[float] = mapped_column(Float)

    @classmethod
    def get_weather_record_on_date(cls, date: str) -> DataFrame:
        formatted_date = datetime.strptime(date, "%Y-%m-%d")
        stmt = (
            select(cls)
            .where(cls.date >= formatted_date)
            .where(cls.date < formatted_date + timedelta(1))
        )
        engine = create_engine("sqlite:///weather.db")
        with engine.connect() as cursor:
            return pd.DataFrame(cursor.execute(stmt))


class NOAAStationMonthlySummary(Base):
    __tablename__ = "noaa_monthly_summary"
    __table_args__ = (
        UniqueConstraint("location_id", "date", name="uq_noaa_location_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Normalize location fields: replace LATITUDE, LONGITUDE, NAME with foreign key
    location_id: Mapped[int] = mapped_column(Integer, ForeignKey("location.id"))
    # DATE refers to the month represented; store as first day of month (Date)
    date: Mapped[date] = mapped_column(Date)

    # Metrics from NOAA CSV (all nullable because dataset can have gaps)
    ADPT: Mapped[float] = mapped_column(Float, nullable=True)  # Avg Dew Point Temp
    AWND: Mapped[float] = mapped_column(Float, nullable=True)  # Avg Wind Speed
    CDSD: Mapped[float] = mapped_column(Float, nullable=True)  # Cooling Degree Days (season-to-date)
    DP01: Mapped[float] = mapped_column(Float, nullable=True)  # Days >= 0.01 in precip
    DP1X: Mapped[float] = mapped_column(Float, nullable=True)  # Days >= 1.00 in precip
    DSND: Mapped[float] = mapped_column(Float, nullable=True)  # Days snow depth >= 1 in
    DSNW: Mapped[float] = mapped_column(Float, nullable=True)  # Days snowfall >= 1 in
    DT00: Mapped[float] = mapped_column(Float, nullable=True)  # Days max temp <= 0 F
    DT32: Mapped[float] = mapped_column(Float, nullable=True)  # Days min temp <= 32 F
    DX32: Mapped[float] = mapped_column(Float, nullable=True)  # Days max temp <= 32 F
    DX70: Mapped[float] = mapped_column(Float, nullable=True)  # Days max temp >= 70 F
    DX90: Mapped[float] = mapped_column(Float, nullable=True)  # Days max temp >= 90 F
    EMNT: Mapped[float] = mapped_column(Float, nullable=True)  # Extreme min temp
    EMXP: Mapped[float] = mapped_column(Float, nullable=True)  # Highest daily precip total
    EMXT: Mapped[float] = mapped_column(Float, nullable=True)  # Extreme max temp
    PRCP: Mapped[float] = mapped_column(Float, nullable=True)  # Total Monthly Precipitation
    PSUN: Mapped[float] = mapped_column(Float, nullable=True)  # Avg daily percent sunshine
    SNOW: Mapped[float] = mapped_column(Float, nullable=True)  # Total Monthly Snowfall
    TAVG: Mapped[float] = mapped_column(Float, nullable=True)  # Average Monthly Temperature
    TMAX: Mapped[float] = mapped_column(Float, nullable=True)  # Monthly Maximum Temperature (avg of daily max)
    TMIN: Mapped[float] = mapped_column(Float, nullable=True)  # Monthly Minimum Temperature (avg of daily min)
    TSUN: Mapped[float] = mapped_column(Float, nullable=True)  # Daily total sunshine in minutes


@dataclass
class HourlyWeatherRecordInstance:
    location_id: int
    date: datetime
    temperature: float
    precipitation: float
    wind_speed: float


@dataclass
class DailyWeatherRecordInstance:
    location_id: int
    date_time: datetime
    month: int
    day_of_month: int
    year: int
    average_temperature: float
    min_temperature: float
    max_temperature: float
    average_wind_speed: float
    min_wind_speed: float
    max_wind_speed: float
    precipitation_sum: float
    precipitation_min: float
    precipitation_max: float

    @classmethod
    def to_dataframe(cls) -> DataFrame:
        return DataFrame(cls)
