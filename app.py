from requests import get
from influxdb import InfluxDBClient
from jsonextract import json_extract
import uuid
import random
import time
import json
from datetime import datetime

areacodes = ['E02002332', 'E02002333', 'E02001098']
metrics = ['newCasesBySpecimenDateRollingSum', 'newCasesBySpecimenDateRollingRate', 'newCasesBySpecimenDateChange', 'newCasesBySpecimenDateChangePercentage']
api_schema = ['areaName','date']
api_schema.extend(metrics)
alldata_dict = {}
linedatalist = []
client = InfluxDBClient(host='192.168.8.100', port=8086)

def get_data(url):
	response = get(url, timeout=10)
    
	if response.status_code >= 400:
		raise RuntimeError(f'Request failed: { response.text }' + 'URL: ' + url)

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

def import_data(jblock):
	# Define local variables

	# Read the capture and extract the required parameters into separate lists
	#jblock = json.loads(data)
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


#Iterate through each area code
for areacode in areacodes:
	#Get the response from the API for the MSOA
	data = get_msoa_data(areacode)
	#Import the relevant JSON data into a dictionary
	data_dict = import_data(data)
	for index in range(len(data_dict)):
		if not index == 0:
			#Create a list formatted for influxDB import in Line protocol format
			linedata = 'msoa-covid-data,location=' + data_dict[index]['areaName'].replace(' ', '') + ' '
			i = 0
			for metric in metrics:
				i += 1
				linedata += metric + '=' + str(data_dict[index][metric])
				if not i == len(metrics):
					linedata += ','
				else:
					linedata += ' '
			linedata += str(data_dict[index]['date'])
			linedatalist.append(linedata)
	#		dstindex = len(alldata_dict) + 1
	#		alldata_dict[dstindex] = data_dict[index]

#areacode = 'E02002332'
#metric = 'newCasesBySpecimenDateRollingSum'


#print(data)


#print(linedatalist)
#Write the data to InfluxDB
client.write_points(linedatalist, database='covid', time_precision='s', batch_size=10000, protocol='line')