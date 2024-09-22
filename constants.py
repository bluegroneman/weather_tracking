from sqlalchemy import create_engine
from os import path


LATITUDE: float = 42.8330
LONGITUDE: float = 108.7307
ENGINE = create_engine("sqlite:///weather.db")
ROOT_DIR = path.dirname(path.abspath(__file__))
START_DATE = "2019-06-20"
END_DATE = "2024-06-20"
