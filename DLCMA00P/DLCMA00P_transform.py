import json
import csv
import datetime, time
import os, sys
import requests
import regex
from cerberus import Validator
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLCMA00P_map import *
from DLCMA00P_format import *

# Globals
DLCMA00P_encoding = {'D1SEQDM': 'ascii','D1CONO': 'ascii','D1EXCD': 'ascii','D1SAL': 'ascii','D1FMNM': 'ascii','D1MDNM': 'ascii','D1GVNM': 'ascii','D1NMSFX': 'ascii','D1FNM': 'ascii','D1COMP': 'ascii','D1DPNM': 'ascii','D1POSN': 'ascii','D1EMLA': 'ascii','D1PHN1': 'ascii','D1PHN2': 'ascii','D1PHN3': 'ascii','D1PHN4': 'ascii','D1PHN5': 'ascii','D1PHN6': 'ascii','D1PLNG': 'ascii','D1SRFD': 'ascii','D1CTSTS': 'ascii','D1CRTZ': 'ascii','D1CRTU': 'ascii','D1CHGZ': 'ascii','D1CHGU': 'ascii','D4ADRDSC': 'ascii','D4ADRFMT': 'ascii','D4ADRL0': 'ascii','D4ADRL1': 'ascii','D4ADRL2': 'ascii','D4ADRL3': 'ascii','D4ADRL4': 'ascii','D4ADRL5': 'ascii','D4ADRL6': 'ascii','D4STS': 'ascii','CECN': 'ascii','CECT': 'ascii','DGURL': 'ascii'}
DLCMA00P_validator_schema = {'D1SEQDM': {'type': 'string','maxlength': 10,'empty':False},'D1CONO': {'type': 'string','maxlength': 2,'empty':True},'D1EXCD': {'type': 'string','maxlength': 1,'empty':True},'D1SAL': {'type': 'string','maxlength': 10,'empty':True},'D1FMNM': {'type': 'string','maxlength': 20,'empty':True},'D1MDNM': {'type': 'string','maxlength': 20,'empty':True},'D1GVNM': {'type': 'string','maxlength': 20,'empty':True},'D1NMSFX': {'type': 'string','maxlength': 5,'empty':True},'D1FNM': {'type': 'string','maxlength': 60,'empty':True},'D1COMP': {'type': 'string','maxlength': 60,'empty':True},'D1DPNM': {'type': 'string','maxlength': 60,'empty':True},'D1POSN': {'type': 'string','maxlength': 20,'empty':True},'D1EMLA': {'type': 'string','maxlength': 50,'empty':True},'D1PHN1': {'type': 'string','maxlength': 20,'empty':True},'D1PHN2': {'type': 'string','maxlength': 20,'empty':True},'D1PHN3': {'type': 'string','maxlength': 20,'empty':True},'D1PHN4': {'type': 'string','maxlength': 20,'empty':True},'D1PHN5': {'type': 'string','maxlength': 20,'empty':True},'D1PHN6': {'type': 'string','maxlength': 20,'empty':True},'D1PLNG': {'type': 'string','maxlength': 30,'empty':True},'D1SRFD': {'type': 'string','maxlength': 30,'empty':True},'D1CTSTS': {'type': 'string','maxlength': 1,'empty':True},'D1CRTZ': {'type': 'string','maxlength': 45,'empty':True},'D1CRTU': {'type': 'string','maxlength': 10,'empty':True},'D1CHGZ': {'type': 'string','maxlength': 10,'empty':True},'D1CHGU': {'type': 'string','maxlength': 45,'empty':True},'D4ADRDSC': {'type': 'string','maxlength': 60,'empty':True},'D4ADRFMT': {'type': 'string','maxlength': 1,'empty':True},'D4ADRL0': {'type': 'string','maxlength': 60,'empty':True},'D4ADRL1': {'type': 'string','maxlength': 60,'empty':True},'D4ADRL2': {'type': 'string','maxlength': 60,'empty':True},'D4ADRL3': {'type': 'string','maxlength': 60,'empty':True},'D4ADRL4': {'type': 'string','maxlength': 20,'empty':True},'D4ADRL5': {'type': 'string','maxlength': 20,'empty':True},'D4ADRL6': {'type': 'string','maxlength': 60,'empty':True},'D4STS': {'type': 'string','maxlength': 1,'empty':True},'CECN': {'type': 'string','maxlength': 10,'empty':True},'CECT': {'type': 'string','maxlength': 3,'empty':True},'DGURL': {'type': 'string','maxlength': 120,'empty':True}}
DLCMA00P_record = DLCMA00P_encoding.keys()

log_messages={}
llmigration_table= 'contact_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/DLCMA00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/DLCMA00P_output_test.tsv'
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
    global insert_count
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
        
def DLCMA00P_validate_fields(record):
    global skip_record
    # field specific mapping
    
           
### MAIN ###  
# field validator setup
v = Validator(DLCMA00P_validator_schema)
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
    contact_master = csv.DictReader(csv_file, delimiter=',')
        
    for row in contact_master:
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        line_count += 1
        
        # initialize output_record keys
        for x in range(0, len(DLCMA00P_Field_format),3):
            output_record[DLCMA00P_Field_format[x]] = ''
        
        for col in field_map.keys():
            # move data to output column
            output_record[col] = row[field_map[col]]

        #check all fields
        DLCMA00P_validate_fields(output_record)
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                #log_messages['Record ID'] = output_record['P9PI'] + ':' + output_record['P9CI']
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