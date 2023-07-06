import pandas as pd
import os
from datetime import datetime
import pathlib
import boto3
from botocore.config import Config
from datetime import timezone
from dateutil.relativedelta import relativedelta
import json
import urllib.parse


client = boto3.client('timestream-write')
INSERT_BATCH_SIZE = 99
NUMBER_OF_MONTHS_TO_PROCESS = 3

def formatdate(date,time):
    localdatetime = date + time
    date_time_obj = datetime.strptime(localdatetime, '%Y%m%d%H%M')
    return date_time_obj

def converttofarenheit(celsius_value):
    if celsius_value is not None:
        return (celsius_value * 9/5) + 32
    else:
        return celsius_value
    
def _print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        if "ExistingVersion" in rr:
            print("Rejected record existing version: ", rr["ExistingVersion"])

def upsertrecords(records):
    try:
        result = client.write_records(DatabaseName="soildatabase", TableName="soil_table",
                                            Records=records, CommonAttributes={})
        #print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except client.exceptions.RejectedRecordsException as err:
        _print_rejected_records_exceptions(err)
    except Exception as err:
        print("Error:", err)

def processdata(event, context):
    if event:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        dat_file = "s3://{}/{}".format(bucket,key)
        processfile(dat_file)


def processfile(dat_file):
    client = boto3.client('timestream-write')
    
    records = []
    print("Processing file: {}".format(dat_file))
    
    df = pd.read_csv(
            dat_file, 
            names=['WBANNO','UTC_DATE','UTC_TIME','T_HR_AV','SOIL_TEMP_5','SOIL_TEMP_10'],
            na_values=["-99.000", "-9999.0"],
            dtype = {'WBANNO': str,'UTC_DATE': str, 'UTC_TIME': str},
            delim_whitespace=True,
            header=None,
            usecols=[0,1,2,9,33,34]
    )
    df["T_HR_AV_F"] = df["T_HR_AV"].apply(converttofarenheit)
    df["SOIL_TEMP_5_F"] = df["SOIL_TEMP_5"].apply(converttofarenheit)
    df["SOIL_TEMP_10_F"] = df["SOIL_TEMP_10"].apply(converttofarenheit)
    df["UTC_DATE_AND_TIME"] = df.apply(lambda row: formatdate(row['UTC_DATE'],row['UTC_TIME']), axis=1)
    df = df.drop(columns=['UTC_DATE', 'UTC_TIME','T_HR_AV','SOIL_TEMP_5','SOIL_TEMP_10'])
    
    for index, row in df.iterrows():   
        
        millisec = row['UTC_DATE_AND_TIME'].timestamp() * 1000

        current =  datetime.now(timezone.utc)
        three_month_date = current - relativedelta(months=NUMBER_OF_MONTHS_TO_PROCESS)
        three_month_millis = three_month_date.timestamp() * 1000
        
        if (millisec >= three_month_millis):

            ms_str = str(millisec)[:-2]
            dimensions = [
                {'Name': 'WBANNO', 'Value': row['WBANNO']}
            ]

            soil_5 = {
                'Dimensions': dimensions,
                'MeasureName': 'soil_temp_5',
                'MeasureValue': str(row['SOIL_TEMP_5_F']),
                'MeasureValueType': 'DOUBLE',
                'Time': ms_str
            }

            soil_10 = {
                'Dimensions': dimensions,
                'MeasureName': 'soil_temp_10',
                'MeasureValue': str(row['SOIL_TEMP_10_F']),
                'MeasureValueType': 'DOUBLE',
                'Time': ms_str
            }

            avg_hour_temp = {
                'Dimensions': dimensions,
                'MeasureName': 'avg_hourly_temp',
                'MeasureValue': str(row['T_HR_AV_F']),
                'MeasureValueType': 'DOUBLE',
                'Time': ms_str
            }

            records.append(soil_5)
            records.append(soil_10)
            records.append(avg_hour_temp)

            if len(records) == INSERT_BATCH_SIZE:
                upsertrecords(records)
                records = []
        else:
            pass
            #print("Dates are more than 3 months old")
    
    if len(records) > 0:
        upsertrecords(records)
