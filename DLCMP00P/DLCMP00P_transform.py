### DLCMP00P - A System or a Pack is a concept in which several individual titles can be logicallygrouped and sold as a single unit. 
# A Pack could comprise of several individual books grouped and sold as a single unit, together with various miscellaneous titles such as a display rack or other promotional material, 
# or it could comprise of a book with a cassette and/or video attached. 
# A System is brought into stock in component form; it is stored by component, but sold as one whole unit. 
# An example of this might be books sold as a set, a series or as an encyclopedia.

import json
import csv
import datetime, time
import os, sys
import requests
import regex
from cerberus import Validator
import mysql.connector

# hidden parameters
from llsecrets import *
# field mapper and formats
from DLCMP00P_map import *
from DLCMP00P_format import *

# Globals
DLCMP00P_encoding = {'P9PI': 'ascii', 'P9CI': 'ascii', 'P9TQTY': 'ascii', 'P9PRPC': 'ascii'}
DLCMP00P_validator_schema = {'P9PI': {'type': 'string','maxlength': 20},'P9CI': {'type': 'string','maxlength': 20},'P9TQTY': {'type': 'string','maxlength': 7},'P9PRPC': {'type': 'string','maxlength': 6}}
DLCMP00P_record = DLCMP00P_encoding.keys()

log_messages={}
llmigration_table= 'DLCMP00P_Pack'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMP00P/DLCMP00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMP00P/DLCMP00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
# counters
line_count = 0
write_count = 0
insert_count = 0
skip_count = 0

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
        
# Verify a customer master record exists in the database
def check_item_master(item_key):
    import mysql.connector
    global cursor, skip_record
    item_list = [item_key]
    try:
        qry = 'Select I1I from item_master where I1I = %s'
        cursor.execute(qry, item_list)
        connection.commit()
        item_master_rec = cursor.fetchone()
        if not item_master_rec:
            skip_record = True
            log_messages['item_master not found'] = item_key
    except mysql.connector.DatabaseError as error:
        skip_record = True
        # skip error reporting if record not found in Customer Master
        log_messages['MySQL_query'] = str(error)
        log_messages['item_master not found'] = item_key
        log_json_message(log_messages)    
        
def DLCMP00P_validate_fields(record):
    global skip_record
    # field specific mapping
    
    # format numbers
    if record['P9PRPC']:
        record['P9PRPC'] = '{:.2f}'.format(float(record['P9PRPC']))
    record['P9TQTY'] =  record['P9TQTY'].zfill(7)
           
### MAIN ###  
# field validator setup
v = Validator(DLCMP00P_validator_schema)
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
    cursor = connection.cursor(buffered=True)
    cursor.execute("select database();")
    record = cursor.fetchone()
    # print("You're connected to database: ", record) 

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
        for x in range(0, len(DLCMP00P_Field_format),3):
            output_record[DLCMP00P_Field_format[x]] = ''
        
        for col in field_map.keys():
            # move data to output column
            output_record[col] = row[field_map[col]]

        #check all fields
        DLCMP00P_validate_fields(output_record)
        # check if a Item master record exists
        if not skip_record:
            check_item_master(output_record['P9PI'])
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['P9PI'] + ':' + output_record['P9CI']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                loggily_json_message(log_messages)
            else:
                values = output_record.values()
                database_insert(output_record)
                if not skip_record:
                    csvwriter.writerow(values)
                    write_count += 1
        else:
            skip_count += 1

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    # print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Skipped']= skip_count

log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_json_message(log_messages)
sys.exit()