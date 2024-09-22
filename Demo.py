from models import DailyWeatherRecord
from sqlalchemy import create_engine, select
from pandas import DataFrame

engine = create_engine("sqlite:///weather.db")
stmt = select(DailyWeatherRecord).order_by(DailyWeatherRecord.date_time)
with engine.connect() as conn:
    weather_df = DataFrame(conn.execute(stmt))

print(weather_df)
temperature_record = DailyWeatherRecord().get_weather_record_on_date("2023-01-01")
print(temperature_record)
temperature_on_date = DailyWeatherRecord().get_mean_temperature_in_fahrenheit(
    "2023-01-01"
)
print(temperature_on_date)
max_wind_speed_on_date = DailyWeatherRecord().get_max_wind_speed_on_date("2023-01-01")
print(max_wind_speed_on_date)
precipitation_sum_on_date = DailyWeatherRecord().get_precipitation_sum_on_date(
    "2023-01-01"
)
print(precipitation_sum_on_date)
