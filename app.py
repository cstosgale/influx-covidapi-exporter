#!/usr/bin/env python3

from requests import get
from influxdb import InfluxDBClient
from jsonextract import json_extract
import uuid
import random
import time
import json
import sys
from datetime import datetime

msoa_areacodes = ['E02002332', 'E02002333', 'E02001098']
msoa_metrics = ['newCasesBySpecimenDateRollingSum', 'newCasesBySpecimenDateRollingRate', 'newCasesBySpecimenDateChange', 'newCasesBySpecimenDateChangePercentage']
msoa_api_schema = ['areaName','date']
msoa_api_schema.extend(msoa_metrics)
nation_areacodes = ['E92000001']
nation_metrics = ['newDeaths28DaysByDeathDateAgeDemographics', 'cumAdmissionsByAge']
nation_api_schema = ['areaName','date']
nation_api_schema.extend(nation_metrics)

api_schemas = {
	'areatype': 'nation',
	'areacodes': ['E92000001'],
	'l1tags': ['areaName','date'],
	'l2tags': ['age'],
	'l1metrics': ['newDeaths28DaysByDeathDateAgeDemographics', 'cumAdmissionsByAge'],
	'l2metrics': ['rate', 'value', 'deaths', 'rollingRate', 'rollingSum']
},{
	'areatype': 'msoa',
	'areacodes': ['E02002332', 'E02002333', 'E02001098'],
	'l1tags': ['areaName','date'],
	'l2tags': [],
	'l1metrics': ['newCasesBySpecimenDateRollingSum', 'newCasesBySpecimenDateRollingRate', 'newCasesBySpecimenDateChange', 'newCasesBySpecimenDateChangePercentage'],
	'l2metrics': []
}



alldata_dict = {}
linedatalist = []
client = InfluxDBClient(host='192.168.8.100', port=8086)

def get_data(areatype, areacode, metrics):
	#Gets COVID data for a specific area code for a specific metric	
	endpoint = 'https://api.coronavirus.data.gov.uk/v2/data?areaType=' + areatype + '&areaCode=' + areacode + '&'
	for metric in metrics:
		endpoint += 'metric=' + metric + '&'
	endpoint += 'format=json'
	
	print(endpoint)
	
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
#	print(value)
	if value is None:
		result = 0
	else:
		result = value
	return result
	
def write_line_data(areatype, tags_values, metrics_values):
	#Write metric name
	linedata = areatype + '-covid-data,'
	#Write tags
	i = 0
	for tag_value in tag_values:
		i += 1
		if tag_value['name'] == 'areaName':
			linedata += 'location=' + tag_value['value'].replace(' ','')
		elif tag_value['name'] == 'date':
			linedata += tag_value['name'] + '=' + date_timestamp(tag_value['value'])
		else:
			linedata += tag_value['name'] + '=' + tag_value['value']
		if not i == len(metrics):
			linedata += ','
		else:
			linedata += ' '
	#Write metrics
	i = 0
	for metric_value in metric_values:
		i += 1
		linedata += metric_value['name'] + '=' + metric_value['value']
		if not i == len(metrics):
			linedata += ','
		else:
			linedata += ' '		
	linedatalist.append(linedata)
	

def write_data(api_schemas):
	#Iterate through each schema type
	for api_schema in api_schemas:
		try:
			for areacode in api_schema['areacodes']:
				#Get the response from the API for the each areacode
				data = get_data(api_schema['areatype'], areacode, api_schema['l1metrics'])
				#print(data)
				#Go through each instance in the data
				for index in range(len(data['body'])):
					l1metric_values = []
					l1tag_values = []
					#Go through each L1 Metric and work out if it is a dictionary or not
					for metric in api_schema['l1metrics']:
						metric_dataset = data['body'][index][metric]

						#Check if the metric is a list, or a plain l1 metric
						if isinstance(metric_dataset, list) and len(metric_dataset) > 0:
							l2tag_values = []
							l2metric_values = []
							#print('Its a list! ', data['body'][index][metric])
							#Go through each metric list and pull out the data and tags
							for metric_data in metric_dataset:
								l2tag_values.extend[{'name': 'parentmetric', 'value': metric}]
								for tag in api_schema['l1tags']:
									l2tag_values.extend([{'name': tag, 'value': data['body'][index][tag]}])
								for tag in api_schema['l2tags']:
									l2tag_values.extend([{'name': tag, 'value': metric_data[tag]}])
								for l2metric in api_schema['l2metrics']:
									l2metric_values.extend([{'name': l2metric, 'value': metric_data[l2metric]}])
							write_line_data(api_schema['areatype'], l2tag_values, l2metric_values)
						else:
							l1metric_values.extend([{'name': metric, 'value': metric_dataset}])								
					for tag in api_schema['l1tags']:
						l1tag_values.extend([{'name': tag, 'value': data['body'][index][tag]}])							
					write_line_data(api_schema['areatype'], l1tag_values, l1metric_values)		
		except:
			print('Error: ', sys.exc_info()[0], 'Schema: ', api_schema)
			raise
			
def write_line_data_old2(areatype, areacodes, metrics, api_schema):
	#Iterate through each area code and get the data
	for areacode in areacodes:
		#Get the response from the API for the MSOA
		data = get_data(areatype, areacode, metrics)
		for index in range(len(data['body'])):
			if not index == 0:
				#Create a list formatted for influxDB import in Line protocol format
				linedata = areatype + '-covid-data,location=' + data['body'][index]['areaName'].replace(' ', '') + ' '
				i = 0
				for metric in metrics:
					i += 1
					#If it's an age metric, add a label for the age category and then the metric
					if 'Age' in metric and data['body'][index][metric] is not None:
						for ageindex in range(len(data['body'][index][metric])):
							lineagedata = areatype + '-covid-data,location=' + data['body'][index]['areaName'].replace(' ', '') + ','
							lineagedata += 'age=' + data['body'][index][metric][ageindex]['age'] + ' '
							if 'Deaths' in metric:
								lineagedata += metric + '=' + str(data['body'][index][metric][ageindex]['deaths']) + ' '
							elif 'Admissions' in metric:
								lineagedata += metric + '=' + str(data['body'][index][metric][ageindex]['value']) + ' '
							lineagedata += date_timestamp(data['body'][index]['date'])
							linedatalist.append(lineagedata)
					else:
						linedata += metric + '=' + str(checkfornone(data['body'][index][metric]))
						nonagemetric = True
					if not i == len(metrics):
						linedata += ','
					else:
						linedata += ' '
				linedata += date_timestamp(data['body'][index]['date'])
				if nonagemetric:
					linedatalist.append(linedata)
					nonagemetric = False
				
	#Write the data to InfluxDB
	print(linedatalist)
	#client.write_points(linedatalist, database='covid', time_precision='s', batch_size=10000, protocol='line')

def write_line_data_old(areatype, areacodes, metrics, api_schema):
	#Iterate through each area code and get the data
	for areacode in areacodes:
		#Get the response from the API for the MSOA
		data = get_data(areatype, areacode, metrics)
		for index in range(len(data['body'])):
			if not index == 0:
				#Create a list formatted for influxDB import in Line protocol format
				linedata = areatype + '-covid-data,location=' + data['body'][index]['areaName'].replace(' ', '') + ' '
				i = 0
				for metric in metrics:
					i += 1
					#If it's an age metric, add a label for the age category and then the metric
					if 'Age' in metric and data['body'][index][metric] is not None:
						for ageindex in range(len(data['body'][index][metric])):
							lineagedata = areatype + '-covid-data,location=' + data['body'][index]['areaName'].replace(' ', '') + ','
							lineagedata += 'age=' + data['body'][index][metric][ageindex]['age'] + ' '
							if 'Deaths' in metric:
								lineagedata += metric + '=' + str(data['body'][index][metric][ageindex]['deaths']) + ' '
							elif 'Admissions' in metric:
								lineagedata += metric + '=' + str(data['body'][index][metric][ageindex]['value']) + ' '
							lineagedata += date_timestamp(data['body'][index]['date'])
							linedatalist.append(lineagedata)
					else:
						linedata += metric + '=' + str(checkfornone(data['body'][index][metric]))
						nonagemetric = True
					if not i == len(metrics):
						linedata += ','
					else:
						linedata += ' '
				linedata += date_timestamp(data['body'][index]['date'])
				if nonagemetric:
					linedatalist.append(linedata)
					nonagemetric = False
				
	#Write the data to InfluxDB
	print(linedatalist)
	#client.write_points(linedatalist, database='covid', time_precision='s', batch_size=10000, protocol='line')

write_line_data(api_schemas)
#write_line_data('msoa', msoa_areacodes, msoa_metrics, msoa_api_schema)
#write_line_data('nation', nation_areacodes, nation_metrics, nation_api_schema)