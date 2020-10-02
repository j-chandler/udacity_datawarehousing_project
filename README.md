# Data Warehousing in AWS

## Intro
This project is for managing a Data Warehouse using AWS Redshift's infrastructure. It converts raw data files from two sources, song JSON files available in a musical app named Sparkify and log event JSONs of user activity. These two sources are transformed into a star schema that can be used for analytics.

## Project Structure
- `create_tables.py` builds the tables into Sparkify's star schema
- `etl.py` loads the JSON data into staging tables and transforms the data into the star schema
- `sql_queries.py` houses the queries used to create, drop, transform and insert rows of data to the star schema tables.

## Creating A 'dwh.cfg'
Before the pipeline can be executed you will need to create a config file, named `dwh.cfg`, which points to an AWS Redshift cluster. The config file should be of the following format:

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

## Running The Pipeline
Running the pipeline is simple to do
First run `create_tables.py` to build the tables
Then run `etl.py` to load and insert the data.
You can now execute queries against the tables defined in `sql_queries.py`.

NOTE: Running `create_tables.py` will drop any existing tables before rebuilding them so save any data changes before re-running this.
