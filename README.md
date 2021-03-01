# influx-covidapi-exporter
Python script to export data from data.coronavirus.gov.uk and import it into influxdb

This script will connect to version 2 of the API, pull down the data you request, and import it into InfluxDB. The script is designed to run on a loop, so it can be run as a service, or in a container, and keep the data up to date.

The script will also pull in any historic data available, as the API provides at least 6 months or so of back data. Each time it runs it will import all the data available (although influxDB will not duplicate existing data)

# Configuration

The operation of the script is configured by the data in config.json. This defines:

api_schemas: the schema of the metrics you wish to download
influxdb_settings: the details to connect to influxdb
script_settings: Global settings such as the loop time (how long the script waits between runs

# API Schema

