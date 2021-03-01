#!/usr/bin/env python3

from requests import get
from influxdb import InfluxDBClient
import uuid
import random
import time
import json
import sys
import os
from datetime import datetime

#Open config file
filepath = os.path.dirname(__file__) + '/config.json'
with open(filepath) as config_file:
	config_json = json.load(config_file)
	api_schemas = config_json['api_schemas']
	idb_host = config_json['influxdb_settings']['host']
	idb_port = config_json['influxdb_settings']['port']
	loopsecs = int(config_json['script_settings']['loopsecs'])

timestamp_tag = 'date'
alldata_dict = {}
linedatalist = []
client = InfluxDBClient(host=idb_host, port=idb_port)

def get_data(areatype, areacode, metrics):
	#Gets COVID data for a specific area code for a specific metric	
	endpoint = 'https://api.coronavirus.data.gov.uk/v2/data?areaType=' + areatype + '&areaCode=' + areacode + '&'
	for metric in metrics:
		endpoint += 'metric=' + metric + '&'
	endpoint += 'format=json'
	
	print('Accessing Endpoint: ',endpoint)
	
	response = get(endpoint, timeout=10)
    
	if response.status_code >= 400:
		raise RuntimeError('Request failed: { response.text }' + 'URL: ' + endpoint)

	return response.json()
	
def date_timestamp(string):
	#Converts the frame.time string formatted as "Jan  9, 2021 11:12:52.206763000 GMT Standard Time" to datetime
	i = 0
	datetimestr = ''
	datetime_obj = datetime.now()

	try:
		datetime_obj = datetime.strptime(string, '%Y-%m-%d')
	except Exception as e:
		print('Datetime format error: ',e,'Full String: ', string)
	timestamp = int(datetime.timestamp(datetime_obj))
	return str(timestamp)
	
def checkfornone(value):
	if value is None:
		result = 0
	else:
		result = value
	return str(result)
	
def chunks(lst, n):
    #"""Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
	
def write_line_data(areatype, tags_values, metrics_values, date):
	#Write metric name
	linedata = areatype + '-covid-data,'
	#Write tags
	i = 0
	for tag_value in tags_values:
		i += 1
		if tag_value['name'] == 'areaName':
			linedata += 'location=' + tag_value['value'].replace(' ','')
		else:
			linedata += tag_value['name'] + '=' + tag_value['value']
		if not i == len(tags_values):
			linedata += ','
		else:
			linedata += ' '
	#Write metrics
	i = 0
	for metric_value in metrics_values:
		i += 1
		try:
			linedata += metric_value['name'] + '=' + checkfornone(metric_value['value'])
		except:
			print('Error: ', sys.exc_info()[0])
			print(metric_value['name'])
			print(metric_value['value'])
			raise
		if not i == len(metrics_values):
			linedata += ','
		else:
			linedata += ' '
	#Write timestamp
	linedata += date_timestamp(date)
	linedatalist.append(linedata)
	

#Run the main code block on a loop, every loopsecs seconds
while True:
	#Iterate through each schema type
	print('Collecting data...')
	for api_schema in api_schemas:
		try:
			#Get the response from the API for the each areacode
			for areacode in api_schema['areacodes']:
				#Split the l1metrics into batches of 5 as this is the limit of the API
				for l1metrics in chunks(api_schema['l1metrics'], 5):
					data = get_data(api_schema['areatype'], areacode, l1metrics)
					#print(data)
					#Go through each instance in the data
					for index in range(len(data['body'])):
						l1metrics_values = []
						l1tags_values = []
						#Go through each L1 Metric and work out if it is a dictionary or not
						for metric in l1metrics:
							metric_dataset = data['body'][index][metric]
							#Check if the metric is a list, or a plain l1 metric
							if isinstance(metric_dataset, list) and len(metric_dataset) > 0:
								#Go through each metric list and pull out the data and tags
								for metric_data in metric_dataset:
									l2tags_values = []
									l2metrics_values = []
									l2tags_values.extend([{'name': 'parentmetric', 'value': metric}])
									for tag in api_schema['l1tags']:
										l2tags_values.extend([{'name': tag, 'value': data['body'][index][tag]}])
									for tag in api_schema['l2tags']:
										#Check the tag exists in the data
										if tag in metric_data:
											l2tags_values.extend([{'name': tag, 'value': metric_data[tag]}])
									for l2metric in api_schema['l2metrics']:
										#Check if the metric exists in the data
										if l2metric in metric_data:
											l2metrics_values.extend([{'name': l2metric, 'value': metric_data[l2metric]}])
									write_line_data(api_schema['areatype'], l2tags_values, l2metrics_values, data['body'][index][timestamp_tag])
							elif not isinstance(metric_dataset, list) and metric_dataset is not None:
								l1metrics_values.extend([{'name': metric, 'value': metric_dataset}])								
						for tag in api_schema['l1tags']:
							l1tags_values.extend([{'name': tag, 'value': data['body'][index][tag]}])							
						if len(l1metrics_values) > 0:
							write_line_data(api_schema['areatype'], l1tags_values, l1metrics_values, data['body'][index][timestamp_tag])		
		except:
			print('Error: ', sys.exc_info()[0], 'Schema: ', api_schema)
			raise
			
	#Write the data to InfluxDB
	print('writing the data to InfluxDB Database')
	client.write_points(linedatalist, database='covid', time_precision='s', batch_size=10000, protocol='line')
	#Sleep for loopsec seconds
	print('Sleeping for ', loopsecs/60, 'minutes...')
	time.sleep(loopsecs)