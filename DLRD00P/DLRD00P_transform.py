###  DLRD00P – Sales representative’s definition.
import json
import csv
import datetime
import sys, os
import requests
import regex, re
from cerberus import Validator
import mysql.connector

# hidden parameters
from llsecrets import *
# field mapper and formats
from DLRD00P_map import *

log_messages={}
DLRD00P_encoding ={'D5CLVL': 'ascii','D5CCLT': 'ascii','D5CKEY': 'ascii','D5ILVL': 'ascii','D5ICT': 'ascii','D5IKEY': 'ascii','D5SLRP': 'ascii','D5CLFC': 'ascii'}
DLRD00P_validator_schema = {'D5CLVL': {'type': 'string','maxlength': 2, 'empty':True},'D5CCLT':{'type': 'string','maxlength': 8, 'empty':True},'D5CKEY': {'type': 'string','maxlength': 10, 'empty':False},'D5ILVL': {'type': 'string','maxlength': 2, 'empty':True},'D5ICT': {'type': 'string','maxlength': 8, 'empty':True},'D5IKEY': {'type': 'string','maxlength': 20, 'empty':False},'D5SLRP': {'type': 'string','maxlength': 6, 'empty':True},'D5CLFC': {'type': 'string','maxlength': 5, 'empty':True}}
DLRD00P_record = DLRD00P_encoding.keys()
llmigration_table='sales_rep'

input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLRD00P/DLRD00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLRD00P/DLRD00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False
# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
redact_char = re.compile('[\r\n\t]*')
redact_punct = re.compile(' -.')
# counters
line_count = 0
write_count = 0
insert_count = 0
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
            
def database_insert(insert_record):
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    global insert_count, skip_record
    # fix for utf-8 keys
    columns = columns.replace('\ufeff','')
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        insert_count += 1
    except mysql.connector.DatabaseError as error:
        if not 'Duplicate' in str(error):
            log_messages = {}
            log_messages['MySQL_insert'] = str(error)
            log_json_message(log_messages)
        skip_record = True
        
def DLRD00P_validate_fields(record):
    global skip_record
    # field specific mapping

    # validate fields
    for field_index in range(0, len(DLRD00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        if record[DLRD00P_Field_format[field_index]]:
            log_messages['field'] = DLRD00P_Field_format[field_index]
            if  len(record[DLRD00P_Field_format[field_index]]) >  int(DLRD00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLRD00P_Field_format[field_length]
                skip_record = True
            if DLRD00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLRD00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLRD00P_Field_format[field_index]]
                    skip_record = True
            if DLRD00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLRD00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLRD00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
        
def write_output(record):   
    global skip_record, skipped_count, write_count 
    # Set Default
    if not skip_record:
        DLRD00P_validate_fields(record)
    # output record    
    if not skip_record:
        # validate output record to specification
        if not v.validate(output_record):
            log_messages= v.errors
            log_messages['Status'] = 'record skipped'           
            log_json_message(log_messages)
            skipped_count += 1
            #loggily_json_message(log_messages)
        else:
            values = record.values()
            database_insert(record)
            if not skip_record:
                csvwriter.writerow(values)        
                write_count += 1
    else:
        skipped_count += 1

### MAIN ###  
# field validator setup
v = Validator(DLRD00P_validator_schema)
v.allow_unknown = True
# class_aggregator mapping to use the field name instead of an index

# connect to DB
try: 
    connection = mysql.connector.connect(
        host=llmigration_host,
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
    cursor = connection.cursor(buffered=True)
    cursor.execute("select database();")
    record = cursor.fetchone()

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

with open(input_filename) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLRD00P_Field_format),3):
            #clean_key = DLRD00P_Field_format[x].replace('\ufeff','')
            output_record[DLRD00P_Field_format[x]] = ''
                        
        # Field specific mappings
        output_record['D5CLVL'] = 'C1'
        output_record['D5CCLT'] = ''
        output_record['D5CKEY'] = row['Name ID'].zfill(8)
        output_record['D5ILVL'] = 'I5'
        output_record['D5ICT'] = ''
        output_record['D5CLFC'] = ''
        
                # D5IKEY 
        output_record['D5IKEY'] = row['Company Nos'].zfill(3)
        
        # D5D5SLRP 
        output_record['D5SLRP'] = row['Company Sales Rep IDs']
        # select record for output
        record_qualifier = row['Company Nos'].zfill(2) + output_record['D5SLRP']
        if  not record_qualifier in D5SLRP_qualifier:
            skip_record = True
                
        if not skip_record:
            DLRD00P_validate_fields(output_record)
        # output record    
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                skipped_count += 1
                #loggily_json_message(log_messages)
            else:
                values = output_record.values()
                database_insert(output_record)
                if not skip_record:
                    csvwriter.writerow(values)        
                    write_count += 1
        else:
            skipped_count += 1

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
# close output file
output_file.close()

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()