####  5th pass to incorporate DLCD00P-Book Customer Addresses
####  Delete records from DB - If last-purchase-date is blank or less than 1/1/2017 and create-date less than 1/1/2022 
import json
import csv
import datetime
import os, sys
import requests
import regex
from cerberus import Validator
import mysql.connector

# hidden parameters
from llsecrets import *
from DLCD00P_format import *

# Globals
log_messages={}
DLCD00P_encoding = {'C1CN': 'ascii','C1BN': 'ascii'}
DLCD00P_validator_schema = {'C1CN':{'type':'string','required':True,'maxlength':8},'C1BN':{'type':'string','required':False,'maxlength':10}}
DLCD00P_record = DLCD00P_encoding.keys()
llmigration_table='customer_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/DLCD00P-Book Customer Addresses.csv'
input_filename_2 = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/Book Customer Billing Info.csv'
skip_record = False
customer_info = {}

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')

# date comparisons
purchase_limit = datetime.datetime(2017,1,1)
create_limit = datetime.datetime(2022,1,1)

# counters
line_count = 0
write_count = 0
delete_count = 0
skipped_count = 0
update_count = 0

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
            
def DLCM00P_delete_record(C1CN):
    global delete_count
    # fix for utf-8 keys
    try:
        query = "delete from customer_master where C1CN = %s"
        field_data = (C1CN, )
        cursor.execute(query, field_data)
        connection.commit()
        delete_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_update'] = str(error)
        log_json_message(log_messages)

def DLCM00P_update_record(C1CN, C1BN):
    global update_count
    item_list = [C1BN, C1CN]
    try:
        query = "UPDATE customer_master SET C1BN = %s WHERE C1CN = %s"
        cursor.execute(query, item_list)
        connection.commit()
        update_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_update'] = str(error)
        log_messages['C1CN'] = C1CN
        log_json_message(log_messages)
       
def DLCD00P_validate_fields(record, skip_record):
        if record[DLCD00P_Field_format[field_index]]:
            log_messages['field'] = DLCD00P_Field_format[field_index]
            if  len(record[DLCD00P_Field_format[field_index]]) >  int(DLCD00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLCD00P_Field_format[field_length]
                skip_record = True
            if DLCD00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLCD00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLCD00P_Field_format[field_index]]
                    skip_record = True
            if DLCD00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLCD00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLCD00P_Field_format[field_index]]
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
    #print("You're connected to database: ", record) 

for field_index in range(0, len(DLCD00P_Field_format),3):
    field_type = field_index + 1
    field_length = field_index + 2

# load dates into dictionary for file #1
with open(input_filename) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        #line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLCD00P_Field_format),3):
            output_record[DLCD00P_Field_format[x]] = ''

        # Field specific mappings
        output_record['C1CN'] = row['Customer ID'].zfill(8)
    
        # only update if a primary address record
        if row['Address Type'] == 'PRI':
            create_date = datetime.datetime(1900,1,1)
            purchase_date = datetime.datetime(1900,1,1)
            if row['Last Purchase Date']:
                purchase_date = datetime.datetime.strptime(row['Last Purchase Date'], "%b %d, %Y")
            if row['Create Date']:
                create_date = datetime.datetime.strptime(row['Create Date'], "%m/%d/%Y")
            customer_info[output_record['C1CN']] = [create_date, purchase_date]
        
# load dates into dictionary for file #2
with open(input_filename_2) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        #line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLCD00P_Field_format),3):
            output_record[DLCD00P_Field_format[x]] = ''

        # Field specific mappings
        output_record['C1CN'] = row['Customer Name ID'].zfill(8)
        # conditional update on dates
        try:
            if row['Book Customer Addresses Last Purchase Date']:
                purchase_date = datetime.datetime.strptime(row['Book Customer Addresses Last Purchase Date'], "%b %d, %Y")
            else: 
                purchase_date = customer_info[output_record['C1CN']][1]
        except KeyError:
                purchase_date = purchase_limit
        try:
            if customer_info[output_record['C1CN']][0]:
                create_date = customer_info[output_record['C1CN']][0]
            else:
                create_date = create_limit
        except KeyError:
            create_date = create_limit
        customer_info[output_record['C1CN']] = [create_date, purchase_date]
        # Update the Customer Master record with the account number
        if row['Default Order Bill To ID'] > '0':
            output_record['C1BN'] = row['Default Order Bill To ID']
            DLCM00P_update_record(output_record['C1CN'], output_record['C1BN'])
            
        
# iterate through the dictionary to determine if records should be deleted       
for C1CN, customer_dates in sorted(customer_info.items()):
    create_date = customer_dates[0]
    purchase_date = customer_dates[1]
    # delete the record if purchase date and create data preceed the date limits
    #      if (not purchase_date) or purchase_date < purchase_limit:     
    if purchase_date and purchase_date < purchase_limit: # 1/1/2017
        if create_date < create_limit:
            #print(C1CN, ',', create_date, ',', purchase_date)
            DLCM00P_delete_record(C1CN)        

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    #print("MySQL connection is closed")            

log_messages = {}
log_messages['Record Deletes Requested']= delete_count
log_messages['Record Updates'] = update_count
log_json_message(log_messages)
sys.exit()