from sqlalchemy import create_engine
from os import path


LATITUDE: float = 42.8330
LONGITUDE: float = 108.7307
ENGINE = create_engine("sqlite:///weather.db")
ROOT_DIR = path.dirname(path.abspath(__file__))
START_DATE = "2017-01-01"
END_DATE = "2025-08-02"
