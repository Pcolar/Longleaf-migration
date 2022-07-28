####  5th pass to incorporate DLCD00P-Book Customer Addresses
####  Delete records from DB - If last-purchase-date is blank or less than 1/1/2019 and create-date less than 1/1/2022 
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
from DLCD00P_field_format import *

# Globals
log_messages={}
DLCD00P_encoding = {'C1CN': 'ascii'}
DLCD00P_validator_schema = {'C1CN':{'type':'string','required':True,'maxlength':8}}
DLCD00P_record = DLCD00P_encoding.keys()
llmigration_table='customer_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/DLCD00P-Book Customer Addresses.csv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')

# date comparisons
date_limit = datetime.datetime(2019,1,1)
create_limit = datetime.datetime(2022,1,1)

# counters
line_count = 0
write_count = 0
delete_count = 0
skipped_count = 0

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
            
def database_update(update_record):
    global delete_count
    # fix for utf-8 keys
    try:
        query = "delete from customer_master where C1CN = %s"
        field_data = (update_record['C1CN'], )
        cursor.execute(query, field_data)
        connection.commit()
        delete_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_update'] = str(error)
        log_json_message(log_messages)
        
def DLCD00P_validate_fields(record, skip_record):
        if record[DLCD00P_field_format[field_index]]:
            log_messages['field'] = DLCD00P_field_format[field_index]
            if  len(record[DLCD00P_field_format[field_index]]) >  int(DLCD00P_field_format[field_length]):
                log_messages['length is greater than'] = DLCD00P_field_format[field_length]
                skip_record = True
            if DLCD00P_field_format[field_type] == "A":
                if not alpha.match(record[DLCD00P_field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLCD00P_field_format[field_index]]
                    skip_record = True
            if DLCD00P_field_format[field_type] == "N":
                if not alpha.match(record[DLCD00P_field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLCD00P_field_format[field_index]]
                    skip_record = True
        if skip_record:
            log_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLCD00P_validator_schema)
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

for field_index in range(0, len(DLCD00P_field_format),3):
    field_type = field_index + 1
    field_length = field_index + 2

with open(input_filename) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLCD00P_field_format),3):
            output_record[DLCD00P_field_format[x]] = ''

        # Field specific mappings
        output_record['C1CN'] = row['Customer ID']
        # length of C1CN should be 8
        if len(output_record['C1CN']) < 8:
            output_record['C1CN'] = '{:0>8}'.format(output_record['C1CN'])
        
        # only update if a primary address record
        if (not row['Address Type'] == 'PRI'):
            skip_record = True
        else:
            # only delete If last-purchase-date is blank or less than 1/1/2019 and create-date less than 1/1/2022 
            create_date = datetime.datetime(1900,1,1)
            purchase_date = datetime.datetime(1900,1,1)
            if row['Last Purchase Date']:
                purchase_date = datetime.datetime.strptime(row['Last Purchase Date'], "%b %d, %Y")
            if row['Create Date']:
                create_date = datetime.datetime.strptime(row['Create Date'], "%m/%d/%Y")
            if (not row['Last Purchase Date']) or purchase_date < date_limit: # 1/1/2019
                if create_date < create_limit:
                    print(output_record['C1CN'], ',', create_date, ',', purchase_date)
                    database_update(output_record)
                else:
                    skip_record = True
                
        if skip_record:
                skipped_count += 1
        

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
#log_messages['Records Written to output file']= write_count
log_messages['Record Deletes Requested']= delete_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()