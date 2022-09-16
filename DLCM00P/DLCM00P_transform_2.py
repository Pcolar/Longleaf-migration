####  2nd pass to incorporate DLCD00P-Book Customer Addresses
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
from DLCD00P_map import *
from DLCD00P_format import *

# Globals
log_messages={}
#DLCD00P_encoding = {'C4CN': 'ascii', 'C4DLVN': 'ascii', 'C4DCNM': 'utf-8', 'C4DAD0': 'utf-8', 'C4DAD1': 'utf-8', 'C4DAD2': 'utf-8', 'C4DAD3': 'utf-8', 'C4DAD4': 'utf-8', 'C4DAD5': 'ascii', 'C4DAD6': 'utf-8', 'C4DPHN': 'ascii', 'C4DFAX': 'ascii', 'C4DXNO': 'ascii', 'C4DXLOC': 'ascii', 'C4CSTS': 'ascii', 'C4SRCH': 'utf-8', 'C4CAR': 'ascii', 'C4FAGC': 'ascii', 'C4DSTP': 'ascii', 'C4RUN': 'ascii'}
#DLCD00P_validator_schema = {'C4CN': {'type': 'string','maxlength': 10,'required':True},'C4DLVN': {'type': 'string','maxlength': 3,'required':True},'C4DCNM': {'type': 'string','maxlength': 60},'C4DAD0': {'type': 'string','maxlength': 60},'C4DAD1': {'type': 'string','maxlength': 60},'C4DAD2': {'type': 'string','maxlength': 60},'C4DAD3': {'type': 'string','maxlength': 60},'C4DAD4': {'type': 'string','maxlength': 20},'C4DAD5': {'type': 'string','maxlength': 20},'C4DAD6': {'type': 'string','maxlength': 60},'C4DPHN': {'type': 'string','maxlength': 20},'C4DFAX': {'type': 'string','maxlength': 20},'C4DXNO': {'type': 'string','maxlength': 10},'C4DXLOC': {'type': 'string','maxlength': 20},'C4CSTS': {'type': 'string','maxlength': 1},'C4SRCH': {'type': 'string','maxlength': 10},'C4CAR': {'type': 'string','maxlength': 2},'C4FAGC': {'type': 'string','maxlength': 2},'C4DSTP': {'type': 'string','maxlength': 3},'C4RUN': {'type': 'string','maxlength': 3}}
#DLCD00P_record = list(DLCD00P_encoding.keys())

DLCD00P_encoding = {'C1CN': 'ascii','C1TXNO': 'ascii', 'C1CAR': 'ascii', 'C1MIF': 'ascii', 'C1FSCF': 'ascii'}
DLCD00P_validator_schema = {'C1CN':{'type':'string','required':True,'maxlength':8},'C1TXNO':{'type':'string','empty':True,'maxlength':15},'C1CAR':{'type':'string','empty':True,'maxlength':2},'C1MIF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1FSCF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']}}
DLCD00P_record = list(DLCD00P_encoding.keys())

llmigration_table='customer_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/DLCD00P-Book Customer Addresses.csv'
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
        query = """update customer_master set C1TXNO = %s, C1CAR = %s, C1MIF = %s, C1FSCF = %s where C1CN = %s"""
        field_data = (update_record['C1TXNO'], update_record['C1CAR'], update_record['C1MIF'], update_record['C1FSCF'], update_record['C1CN'])

        cursor.execute(query, field_data)
        connection.commit()
        update_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_update'] = str(error)
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
    #print("You're connected to database: ", record) 

for field_index in range(0, len(DLCD00P_Book_customer_addresses),3):
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
        for x in range(0, len(DLCD00P_record)):
            output_record[DLCD00P_record[x]] = ''

        # Field specific mappings
        # length of C1CN should be 8
        output_record['C1CN'] = row['Customer ID'].zfill(8)
        
        # map update fields
        if row['Customer Tax Exempt Id']:
            output_record['C1TXNO'] = row['Customer Tax Exempt Id'].split('*',1)[1][0:14]
        output_record['C1CAR'] = row['Ship Method IDs'][0:1]
        if int(row['Picklist Minimum Qty']) > 0:
            output_record['C1MIF'] = 'Y'
        else:
            output_record['C1MIF'] = 'N'
            
        if row['Returnable Ind'] == 'R' or row['Returnable Ind'] == 'N':
            output_record['C1FSCF'] = 'Y'
        else:
            output_record['C1FSCF'] = 'N'
        
        # only update if a primary address record
        if (not row['Address Type'] == 'PRI') or row['Name Name Class Id'] == 'IR' or row['Name Name Class Id'] == 'IA':
            skip_record = True
        
        #if not skip_record:
        #   DLCD00P_validate_fields(output_record, skip_record)
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
        else:
            skipped_count += 1

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    #print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
#log_messages['Records Written to output file']= write_count
log_messages['Records Updated']= update_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()