###  DLIME00P - 
import json
import csv
import datetime
import sys
import requests
import regex, re
from cerberus import Validator
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLIME00P_map import *

log_messages={}
DLIME00P_encoding ={'BII': 'ascii','BIRECT': 'ascii','BIPIDD': 'ascii','BIBID': 'ascii','BICT': 'ascii','BISEQ': 'ascii','BISEQDM': 'ascii','BIFNM': 'ascii'}
DLIME00P_validator_schema = {'BII': {'type': 'string','maxlength': 20,'empty':False},'BIRECT': {'type': 'string','maxlength': 1,'empty':True},'BIPIDD': {'type': 'string','maxlength': 6,'empty':True},'BIBID': {'type': 'string','maxlength': 6,'empty':True},'BICT': {'type': 'string','maxlength': 3,'empty':False},'BISEQ': {'type': 'string','maxlength': 5,'empty':True},'BISEQDM': {'type': 'string','maxlength': 10,'empty':False},'BIFNM': {'type': 'string','maxlength': 60,'empty':False}}
DLIME00P_record = DLIME00P_encoding.keys()
llmigration_table='contributors'

input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIME00P/Product Authors - DLIME00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIME00P/DLIME00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
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
        
def DLIME00P_validate_fields(record):
    global skip_record
    # field specific mapping
    # normalize ISBN13

    # validate fields
    for field_index in range(0, len(DLIME00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        if record[DLIME00P_Field_format[field_index]]:
            log_messages['field'] = DLIME00P_Field_format[field_index]
            if  len(record[DLIME00P_Field_format[field_index]]) >  int(DLIME00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLIME00P_Field_format[field_length]
                skip_record = True
            if DLIME00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLIME00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLIME00P_Field_format[field_index]]
                    skip_record = True
            if DLIME00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLIME00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLIME00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
        
def write_output(record):   
    global skip_record, skipped_count, write_count 
    # Set Default
    if not skip_record:
        DLIME00P_validate_fields(record)
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
v = Validator(DLIME00P_validator_schema)
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

with open(input_filename) as csv_file:
    DLIME00P_rec = csv.DictReader(csv_file, delimiter=',')
    for row in DLIME00P_rec:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLIME00P_Field_format),3):
            #clean_key = DLIME00P_Field_format[x].replace('\ufeff','')
            output_record[DLIME00P_Field_format[x]] = ''

        for col in DLIME00P_field_map.keys():
            # fix for utf-8 column names
            col = col.replace('\ufeff','')
            # move data to output column
            output_record[col] = row[DLIME00P_field_map[col]]
            
        # Field specific mappings
        output_record['BII'] = output_record['BII'].replace(' .-','')
        output_record['BIRECT'] = ''
        output_record['BIPIDD'] = ''
        output_record['BIBID'] = ''
        output_record['BISEQ'] = ''
        

        if not skip_record:
            DLIME00P_validate_fields(output_record)
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

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()