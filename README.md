# Weather Forecasting app for Lander Wyoming
The Python scripts in this repository are meant to import a set of data from the Open Meteo Historical Weather Data API.
There are a set of [tests](test_weather_records.py) that can be ran after importing the data set initially.

## Importing the data set
**Clone this repository**
Use SQLite to create a database with the name of `weather.db`
```shell
$ sqlite3 weather.db
sqlite> .exit
```
If on Windows and using Powershell use the following command to create an empty database
```shell
$ sqlite3 weather.db " "
```
Activate the virtual environment and install packages from the Pipfile
```shell
$ pipenv install
$ pipenv shell
```
Run the initial migration to create the database schema and add location data
```shell
$ python main.py -m
```
Run the data import script to import the last five years worth of hourly weather data from the Open Meteo API
```shell
$ python main.py -i
```
Populate the daily weather records into the daily_weather table;
```shell
$ python main.py -b
```

This project now uses Typer for the CLI. You can see available options with:
```shell
$ python main.py --help
```
In a new SQL console or Database navigator validate the table was populated with the correct columns
```sql
select * from daily_weather;
select * from hourly_weather;
select * from location;
```
## Testing and exploring the dataset
Run the test suite to ensure the data populated accordingly
```shell
$ pytest
```
As long as tests are passing, you can run the `Demo.py` script to see the data.
For further exploration it is recommended to use a Jupyter notebook to explore the data.
```shell
$ python Demo.py
```

## Dependencies
- [sqlalchemy](https://pypi.org/project/SQLAlchemy/)
- [openmeteo-requests](https://pypi.org/project/openmeteo-requests/)
- [requests-cache](https://pypi.org/project/requests-cache/)
- [retry-requests](https://pypi.org/project/retry-requests/)
- [numpy](https://pypi.org/project/numpy/)
- [pandas](https://pypi.org/project/pandas/)
- [typer](https://pypi.org/project/typer/)
- [ruff](https://pypi.org/project/ruff/)
- [pytest](https://pypi.org/project/pytest/)
- [pytest-sugar](https://pypi.org/project/pytest-sugar/)