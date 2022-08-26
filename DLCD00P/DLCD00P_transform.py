### DLCD00P - Delivery Address file
### Note: input file requires pre-processing via EXCEL
### input file prep
### Sort by
###     Customer ID Asc
###     Address type Desc
###     Address Line 1
###     Address Line 2	
###     Address Line 3	
###     City	
###     Zip Code
###     Address Id ASC
###
### 	Data -> Tables -> remove duplicates
### 		Columns A, E, G, H, J, M
###     format numeric fields as 'number, zero decimal places'
###     By default, a number over 12 digits in an Excel spreadsheet is auto-formatted to scientific notation.

import json
import csv
import os, sys
import datetime
import requests
import regex, re
from cerberus import Validator
import phonenumbers
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLCD00P_map import *
from DLCD00P_format import *

# Globals
DLCD00P_encoding = {'C4CN': 'ascii', 'C4DLVN': 'ascii', 'C4DCNM': 'utf-8', 'C4DAD0': 'utf-8', 'C4DAD1': 'utf-8', 'C4DAD2': 'utf-8', 'C4DAD3': 'utf-8', 'C4DAD4': 'utf-8', 'C4DAD5': 'ascii', 'C4DAD6': 'utf-8', 'C4DPHN': 'ascii', 'C4DFAX': 'ascii', 'C4DXNO': 'ascii', 'C4DXLOC': 'ascii', 'C4CSTS': 'ascii', 'C4SRCH': 'utf-8', 'C4CAR': 'ascii', 'C4FAGC': 'ascii', 'C4DSTP': 'ascii', 'C4RUN': 'ascii'}
DLCD00P_validator_schema = {'C4CN': {'type': 'string','maxlength': 10,'required':True},'C4DLVN': {'type': 'string','maxlength': 3,'required':True},'C4DCNM': {'type': 'string','maxlength': 60},'C4DAD0': {'type': 'string','maxlength': 60},'C4DAD1': {'type': 'string','maxlength': 60},'C4DAD2': {'type': 'string','maxlength': 60},'C4DAD3': {'type': 'string','maxlength': 60},'C4DAD4': {'type': 'string','maxlength': 20},'C4DAD5': {'type': 'string','maxlength': 20},'C4DAD6': {'type': 'string','maxlength': 60},'C4DPHN': {'type': 'string','maxlength': 20},'C4DFAX': {'type': 'string','maxlength': 20},'C4DXNO': {'type': 'string','maxlength': 10},'C4DXLOC': {'type': 'string','maxlength': 20},'C4CSTS': {'type': 'string','maxlength': 1},'C4SRCH': {'type': 'string','maxlength': 10},'C4CAR': {'type': 'string','maxlength': 2},'C4FAGC': {'type': 'string','maxlength': 2},'C4DSTP': {'type': 'string','maxlength': 3},'C4RUN': {'type': 'string','maxlength': 3}}
DLCD00P_record = DLCD00P_encoding.keys()

log_messages={}
llmigration_table= 'delivery_address'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCD00P/DLCD00P-dedup.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCD00P/DLCD00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
pattern = regex.compile(['\n','\u00017','\ufeff'])
# counters
line_count = 0
write_count = 0
insert_count = 0
address_seq = 0

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
    log_json_message(log_message)
    log_message={}
            
def database_insert(insert_record):
    global insert_count, skip_record
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    # fix for utf-8 keys
    columns = columns.replace('\ufeff','')
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        insert_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_insert'] = str(error)
        log_json_message(log_messages)
        skip_record = True
        
# Verify an customer master record exists in the database
def check_customer_master(item_key):
    import mysql.connector
    global cursor, skip_record
    
    try:
        qry = 'Select C1CN from customer_master where C1CN = %s'
        cursor.execute(qry, item_key)
        connection.commit()
        cust_master_rec = cursor.fetchall()
    except mysql.connector.DatabaseError as error:
        skip_record = True
        log_messages['MySQL_query'] = str(error)
        log_messages['customer_master record not found'] = item_key
        log_json_message(log_messages)    
        
def DLCD00P_validate_fields(record, address_seq, skip_record):
    # field specific mapping
    # length of C4CN should be 8
    while len(record['C4CN']) < 8:
        record['C4CN'] = '0' + record['C4CN']
    
    log_messages['C4CN'] = record['C4CN']
    
    # field specifics
    # verify phone and fax number formats
    if len(record['C4DPHN']) > 0:
        try:
            parsed_phone = phonenumbers.parse(record['C4DPHN'], record['C4DAD6'])
            phonenumbers.is_valid_number(parsed_phone)
        except:
            log_messages['invalid phone number redacted'] = record['C4DPHN']
            record['C4DPHN'] = ''
            log_json_message(log_messages)

    if len(record['C4DFAX']) > 0:
        try:
            parsed_phone = phonenumbers.parse(record['C4DFAX'], record['C4DAD6'])
            phonenumbers.is_valid_number(parsed_phone)      
        except:
            log_messages['invalid fax number redacted'] = record['C4DFAX']
            record['C4DFAX'] = ''
            log_json_message(log_messages)
            
    # format sequence number
    record['C4DLVN'] = str(address_seq)
    if len(record['C4DLVN']) < 3:
        record['C4DLVN'] = '{:0>3}'.format(record['C4DLVN'])
    
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

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

log_messages['File created'] = output_filename
log_json_message(log_messages)

with open(input_filename) as csv_file:
    deliv_addr = csv.DictReader(csv_file, delimiter=',')
    
    # used to track if the same customer ID is seen again
    previous_C4CN = 99999999
    
    for row in deliv_addr:
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        line_count += 1
        
        # initialize output_record keys
        for x in range(0, len(DLCD00P_Field_format),3):
            output_record[DLCD00P_Field_format[x]] = ''
        
        if row['Do Not Use Indicator'] == 'Y' or row['Customer ID'] == '':
            skip_record = True
        else:
            for col in field_map.keys():
                # move data to output column
                output_record[col] = row[field_map[col]]
                # normalize content
                output_record[col] = re.sub(pattern,'',output_record[col])                
                
            # merge Phone and Fax area + number fields
            output_record['C4DPHN'] = row['Telephone Area'] + ' ' + row['Telephone Number']
            output_record['C4DFAX'] = row['Fax Area'] + ' ' + row['Fax Number']
            # trim numbers
            output_record['C4DPHN'] = output_record['C4DPHN'].strip()
            output_record['C4DFAX'] = output_record['C4DFAX'].strip()
            
            # if the 'Book Customer Addresses Attention' field is not blank, shift the addresses lines by 1
            if row['Attention']:
                if output_record['C4DAD2']:
                    log_messages['Record ID'] = output_record['C4CN']
                    log_messages['Attention Field'] = row['Attention']
                    log_messages['Status'] = 'no room for field'
                else:
                    output_record['C4DAD2'] = output_record['C4DAD1']
                    output_record['C4DAD1'] = output_record['C4DAD0']
                    output_record['C4DAD0'] = row['Attention']
                    
            # determine sequence number
            if int(previous_C4CN) == int(output_record['C4CN']):
                # increment sequence count
                address_seq += 1
            else:      
                #if row['Address Description'].startswith('B'):
                address_seq = 0

            #check all fields
            DLCD00P_validate_fields(output_record, address_seq, skip_record)
            
            # verify a corresponding customer master record exists
            check_customer_master(output_record['C4CN'])
            
            # flag address seq GT 100 and skip record
            if address_seq > 100:
                skip_record = True
                log_messages['Address count'] = address_seq
                log_messages['Record ID'] = output_record['C4CN'] + ':' + output_record['C4DLVN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['C4CN'] + ':' + output_record['C4DLVN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
            else:
                values = output_record.values()
                database_insert(output_record)
                previous_C4CN = output_record['C4CN']
                if not skip_record:
                    csvwriter.writerow(values)
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