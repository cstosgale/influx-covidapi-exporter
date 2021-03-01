# influx-covidapi-exporter
Python script to export data from data.coronavirus.gov.uk and import it into influxdb

This script will connect to version 2 of the API, pull down the data you request, and import it into InfluxDB. The script is designed to run on a loop, so it can be run as a service, or in a container, and keep the data up to date.

The script will also pull in any historic data available, as the API provides at least 6 months or so of back data. Each time it runs it will import all the data available (although influxDB will not duplicate existing data)

## Configuration

N.B. A Sample config file is provided, please rename this to config.json for the script to work correctly

The operation of the script is configured by the data in config.json. This defines:

api_schemas: the schema of the metrics you wish to download
influxdb_settings: the details to connect to influxdb
script_settings: Global settings such as the loop time (how long the script waits between runs

## API Schema

This defines what data to pull from the API. As version 2 of the API is not currently documented, you can largely work out what settings to use for new metrics by examining the URL and JSON output from https://coronavirus.data.gov.uk/details/download

areatype: Specifies the area type for the request. There should be a different dictionary for each area type
areacodes: Specifies a list of the areacodes you want to lookup against. These can be worked out by picking the area you want on the URL above, and examining the URL
l1tags: Shouldn't need to be modified. Specifies the tags applied in InfluxDB to all metrics
l1metrics: These are level 1 metrics, so ones that appear one level under the body in the JSON response
l2tags: These are level 2 tags, which appear two levels underthe body in the JSON response. In my example I have used age which is used by some of the metrics
l2metrics: These are level 2 metrics, which appear two levels underthe body in the JSON response.

## Requirements

Please ensure that the latest version of python3 is installed, and that the influxdb and request libraries are installed:

`pip3 install requests
pip3 install influxdb`

## Operation

Before use, setup your config.json file, ensuring it is in the same directory as the app.py script.

Also, ensure that that influxdb and request libraries need to be installed using pip

To run, the script can be run directly using:

`python3 app.py`

It is recommended however that the script is run in a Docker container. This can be done easily by using https://github.com/cstosgale/python-docker

Simply mount this project folder under /bin!
