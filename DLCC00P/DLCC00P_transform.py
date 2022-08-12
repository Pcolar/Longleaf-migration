import json
import csv
import datetime, time
import os, sys
import requests
import regex
from cerberus import Validator
import phonenumbers
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLCC00P_map  import *
from DLCC00P_format import *

# Globals
log_messages={}
DLCC00P_encoding = {'C5CN': 'ascii','C5CTYP': 'ascii','C5EFDT': 'ascii','C5CODE': 'ascii'}
DLCC00P_validator_schema = {'C5CN':{'type':'string','required':True,'maxlength':10},'C5CTYP':{'type':'string','required':True,'maxlength':8},'C5EFDT':{'type':'string','required':True,'maxlength':10},'C5CODE':{'type':'string','required':True,'maxlength':3}}
DLCC00P_record = DLCC00P_encoding.keys()
llmigration_table='customer_classification'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCC00P/Customer Master - DLCM00P 220731.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCC00P/DLCC00P-220804.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
# counters
line_count = 0
write_count = 0
insert_count = 0

def log_json_message(log_message):
    """print out  in json tagged log message format"""
    log_message['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_message['program'] = os.path.basename(__file__)
    print(json.dumps(log_message))
    log_message={}
    
def loggily_json_message(log_message):
    """Push message to Loggily in json tagged log message format"""
    log_message['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    payload=json.dumps(log_message)
    response = requests.post(loggily_URI, data=payload)
    if response.status_code != 200:
        print(response)
    log_message={}
            
def database_insert(insert_record):
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    global insert_count
    # fix for utf-8 keys
    columns = columns.replace('','')
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        insert_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_insert'] = str(error)
        log_json_message(log_messages)
        
def DLCC00P_validate_fields(record, skip_record):
    # field specific mapping
    # length of C5CN should be 8
    while len(record['C5CN']) < 8:
        record['C5CN'] = '{:0>8}'.format(record['C5CN'])
    log_messages['C5CN'] = record['C5CN']
    
    # field specifics            
    # validate fields
    for field_index in range(0, len(DLCC00P_field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        # print(f' index:', field_index, 'key:', DLCC00P_field_format[field_index], 'Type: ', DLCC00P_field_format[field_type], 'Length: ', DLCC00P_field_format[field_length])
        if record[DLCC00P_field_format[field_index]]:
            log_messages['field'] = DLCC00P_field_format[field_index]
            if  len(record[DLCC00P_field_format[field_index]]) >  int(DLCC00P_field_format[field_length]):
                log_messages['length is greater than'] = DLCC00P_field_format[field_length]
                skip_record = True
            if DLCC00P_field_format[field_type] == "A":
                if not alpha.match(record[DLCC00P_field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLCC00P_field_format[field_index]]
                    skip_record = True
            if DLCC00P_field_format[field_type] == "N":
                if not alpha.match(record[DLCC00P_field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLCC00P_field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLCC00P_validator_schema)
v.allow_unknown = True

# connect to DB
try: 
    connection = mysql.connector.connect(host=llmigration_host,
        database=llmigration_db,
        user=llmigration_user,
        password=llmigration_password)
except mysql.connector.Error as error:
    log_messages['MySQL_connection'] = str(error)
    log_json_message(log_messages)
    print(error)
    exit()
if connection.is_connected():
    db_Info = connection.get_server_info()
    cursor = connection.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record) 

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

log_messages['File created'] = output_filename
log_json_message(log_messages)

with open(input_filename) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLCC00P_field_format),3):
            clean_key = DLCC00P_field_format[x].replace('','')
            output_record[clean_key] = ''

        for col in field_map.keys():
            # fix for utf-8 column names
            col = col.replace('','')
            # move data to output column
            output_record[col] = row[field_map[col]]
            
        #map Name Class to Type and Code
        # create CC-TOB record
        try:
            output_record['C5CTYP'] = CCTOB_map[row['Name Class Id']]['C5CTYP']
            output_record['C5CODE'] = CCTOB_map[row['Name Class Id']]['C5CODE']
        except:
            output_record['C5CTYP'] = 'CC-TOB'
            output_record['C5CODE'] = 'TOB'
        
        output_record['C5EFDT'] = '0001-01-01'
        DLCC00P_validate_fields(output_record, skip_record)            
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['C5CN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                loggily_json_message(log_messages)
            else:
                values = output_record.values()
                csvwriter.writerow(values)
                database_insert(output_record)
                write_count += 1
                
        # create CC-LEG record 
        log_messages = {}
        skip_record = False
        try:
            output_record['C5CTYP'] = CCLEG_map[row['Name Class Id']]['C5CTYP']
            output_record['C5CODE'] = CCLEG_map[row['Name Class Id']]['C5CODE']
        except:
            output_record['C5CTYP'] = 'CC-LEG'
            output_record['C5CODE'] = 'ZZZ'
        
        output_record['C5EFDT'] = '0001-01-01'
        DLCC00P_validate_fields(output_record, skip_record)            
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['C5CN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                loggily_json_message(log_messages)
            else:
                values = output_record.values()
                csvwriter.writerow(values)
                database_insert(output_record)
                write_count += 1        

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    print("MySQL connection is closed")            


log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_json_message(log_messages)
sys.exit()