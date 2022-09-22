####  3rd pass to incorporate Gen Customers Accounts DLCM00P Client info
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
from llsecrets import *
from DLCM00P_client_field_format import *
from DLCM00P_map  import *

# Globals
log_messages={}
DLCM00P_client_encoding = {'C1CN': 'ascii','C1STMT': 'ascii', 'C1PYTC': 'ascii', 'C1CRLM': 'ascii'}
DLCM00P_client_validator_schema = {'C1CN':{'type':'string','required':True,'maxlength':8},'C1STMT':{'type':'string','empty':False,'maxlength':1},'C1PYTC':{'type':'string','empty':False,'maxlength':2},'C1CRLM':{'type':'string','empty':True}}
DLCM00P_client_record = DLCM00P_client_encoding.keys()
llmigration_table='customer_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/Gen Customers Accounts DLCM00P Client info.csv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
# counters
line_count = 0
write_count = 0
update_count = 0
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
    global update_count
    # fix for utf-8 keys
    try:
        query = """update customer_master set C1STMT = %s, C1PYTC = %s, C1CRLM = %s where C1CN = %s"""
        field_data = (update_record['C1STMT'], update_record['C1PYTC'],update_record['C1CRLM'],update_record['C1CN'])
        cursor.execute(query, field_data)
        connection.commit()
        update_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_update'] = str(error)
        log_json_message(log_messages)
        
def DLCM00P_client_validate_fields(record, skip_record):
        if record[DLCM00P_client_field_format[field_index]]:
            log_messages['field'] = DLCM00P_client_field_format[field_index]
            if  len(record[DLCM00P_client_field_format[field_index]]) >  int(DLCM00P_client_field_format[field_length]):
                log_messages['length is greater than'] = DLCM00P_client_field_format[field_length]
                skip_record = True
            if DLCM00P_client_field_format[field_type] == "A":
                if not alpha.match(record[DLCM00P_client_field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLCM00P_client_field_format[field_index]]
                    skip_record = True
            if DLCM00P_client_field_format[field_type] == "N":
                if not alpha.match(record[DLCM00P_client_field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLCM00P_client_field_format[field_index]]
                    skip_record = True
        if skip_record:
            log_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLCM00P_client_validator_schema)
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
    # print("You're connected to database: ", record) 

for field_index in range(0, len(DLCM00P_client_field_format),3):
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
        for x in range(0, len(DLCM00P_client_field_format),3):
            output_record[DLCM00P_client_field_format[x]] = ''

        # Field specific mappings
        output_record['C1CN'] = row['Name Id']
        # length of C1CN should be 8
        output_record['C1CN'] = output_record['C1CN'].zfill(8)
        
        # map update fields
        if row['Statement Ind'] == 'Y':
            output_record['C1STMT'] = '1'
        else:
            output_record['C1STMT'] = '0'
                
        # map C1PYTC
        output_record['C1PYTC'] = '30'
        if row['Terms Code']:
            try: 
                output_record['C1PYTC'] = C1PYTC_map[row['Terms Code']]
            except KeyError:
                log_messages['C1PYTC map failed'] = row['Terms Code']
                log_messages['Record ID'] = output_record['C1CN']
                log_json_message(log_messages)
        # map C1CRLM
        output_record['C1CRLM'] = row['Credit Limit']
        if not skip_record:
            DLCM00P_client_validate_fields(output_record, skip_record)
        # output record    
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['C1CN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                skipped_count += 1
                #loggily_json_message(log_messages)
            else:
                #values = output_record.values()
                #csvwriter.writerow(values)
                database_update(output_record)
                #update_count += 1
        else:
            skipped_count += 1

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    # print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
#log_messages['Records Written to output file']= write_count
log_messages['Records Updated']= update_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()