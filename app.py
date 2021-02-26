#!/usr/bin/env python3

from requests import get
from influxdb import InfluxDBClient
from jsonextract import json_extract
import uuid
import random
import time
import json
from datetime import datetime

msoa_areacodes = ['E02002332', 'E02002333', 'E02001098']
msoa_metrics = ['newCasesBySpecimenDateRollingSum', 'newCasesBySpecimenDateRollingRate', 'newCasesBySpecimenDateChange', 'newCasesBySpecimenDateChangePercentage']
msoa_api_schema = ['areaName','date']
msoa_api_schema.extend(msoa_metrics)
national_areacodes = ['E92000001']
national_metrics = ['newCasesByPublishDateAgeDemographics', 'newCasesByPublishDateAgeDemographics', 'cumAdmissionsByAge']
national_api_schema = ['areaName','date']
national_api_schema.extend(national_metrics)
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


def get_msoa_data(areacode):
	#Gets COVID data for a specific area code for a specific metric
	endpoint = 'https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&areaCode=' + areacode + '&'
	for metric in metrics:
		endpoint += 'metric=' + metric + '&'
	endpoint += 'format=json'
	
	print(endpoint)
	response = get_data(endpoint)

	return response
	

	
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
	return timestamp
	
def checkfornone(value):
#	print(value)
	if value is None:
		result = 0
	else:
		result = value
	return result

def import_data(jblock, api_schema):
	# Define local variables

	# Read the capture and extract the required parameters into separate lists
	data_dict = {}
	for sitem in api_schema:
		index = 0
		for jitem in json_extract(jblock, sitem):
			if sitem == 'date':
				jitem = date_timestamp(jitem)
			index += 1
			if len(data_dict) < index:
				data_dict[index] = {}
			data_dict[index][sitem] = checkfornone(jitem)
	return data_dict


#Iterate through each area code and get MSOA data
for areacode in msoa_areacodes:
	#Get the response from the API for the MSOA
	data = get_data('msoa', areacode, msoa_metrics)
	#Import the relevant JSON data into a dictionary
	data_dict = import_data(data, msoa_api_schema)
	for index in range(len(data_dict)):
		if not index == 0:
			#Create a list formatted for influxDB import in Line protocol format
			linedata = 'msoa-covid-data,location=' + data_dict[index]['areaName'].replace(' ', '') + ' '
			i = 0
			for metric in msoa_metrics:
				i += 1
				linedata += metric + '=' + str(data_dict[index][metric])
				if not i == len(msoa_metrics):
					linedata += ','
				else:
					linedata += ' '
			linedata += str(data_dict[index]['date'])
			linedatalist.append(linedata)

#Iterate through each area code and get national data



#Write the data to InfluxDB
client.write_points(linedatalist, database='covid', time_precision='s', batch_size=10000, protocol='line')