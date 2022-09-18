import json
import csv
import datetime
import string
import sys
import requests
import regex, re
from cerberus import Validator
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats

from DLTXT00P_maps import *
from DLTXT00P_format import *

# Globals
log_messages={}
DLTXT00P_encoding ={'TXTTYP':'ascii','TXTKEY':'ascii','TXSEQ':'ascii','TXTXT':'ascii','TXPOSF':'ascii'}
DLTXT00P_validator_schema = {'TXTTYP':{'type':'string','maxlength':3,'empty':True},'TXTKEY':{'type':'string','maxlength':50,'empty':False},'TXSEQ':{'type':'string','maxlength':5,'empty':False},'TXTXT':{'type':'string','maxlength':72,'empty':True},'TXPOSF':{'type':'string','maxlength':1,'empty':True}}
DLTXT00P_record = DLTXT00P_encoding.keys()
llmigration_table='text_file'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLTXT00P/DLIM00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLTXT00P/DLTXT00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
redact_char = re.compile('[\r\n\t]*')
# counters
line_count = 0
write_count = 0
inserted_count = 0
skip_count = 0

def log_json_message():
    global log_messages
    """print out  in json tagged log message format"""
    # log_messages['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    print(json.dumps(log_messages))
    log_messages={}
    
def loggily_json_message():
    global log_messages
    """Push message to Loggily in json tagged log message format"""
    log_messages['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    payload=json.dumps(log_messages)
    response = requests.post(loggily_URI, data=payload)
    if response.status_code != 200:
        log_messages['loggily error'] = response
    log_json_message()
    log_messages={}
            
def database_insert(insert_record):
    global skip_record, skip_count
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    # fix utf-8 column names
    columns = columns.replace('\ufeff','')
    global inserted_count, pub_status_code, last_sold_date
    try:
        qry = 'INSERT INTO text_file VALUES (%s);' % placeholders
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        inserted_count += 1
    except mysql.connector.DatabaseError as error:
        if 'Duplicate entry' in str(error):
            skip_count += 1
        log_messages['MySQL_insert'] = str(error)
        log_messages['Record ID'] = insert_record['TXTKEY']
        log_json_message()
        skip_record = True
        #sys.exit()
        
def DLTXT00P_validate_fields(record):
    global skip_record, item_group
    # field specific mapping
                      
    # Set default values 
    record['TXSEQ'] = '00000'
    record['TXPOSF'] = 'N'
    record['TXTTYP'] = 'IDL'
    # eliminate whitespace, in and out of quotes, and maxlength 72
    record['TXTXT'] = re.sub('\t','',record['TXTXT'])
    #record['TXTXT'] = record['TXTXT'].replace('" ','"')
    # record['TXTXT'] = record['TXTXT'].replace(' "','"')
    record['TXTXT'] = record['TXTXT'].lstrip()[0:71]
    
        # validate fields
    for field_index in range(0, len(DLTXT00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        if record[DLTXT00P_Field_format[field_index]]:
            if  len(record[DLTXT00P_Field_format[field_index]]) >  int(DLTXT00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLTXT00P_Field_format[field_length]
                skip_record = True
            if DLTXT00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLTXT00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLTXT00P_Field_format[field_index]]
                    skip_record = True
            if DLTXT00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLTXT00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLTXT00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message()
    
    
            
### MAIN ###  
# field validator setup
v = Validator(DLTXT00P_validator_schema)
v.allow_unknown = True

# connect to DB
try: 
    connection = mysql.connector.connect(host=llmigration_host,
        database=llmigration_db,
        user=llmigration_user,
        password=llmigration_password)
except mysql.connector.Error as error:
    log_messages['MySQL_connection'] = str(error)
    log_json_message()
    print(error)
    exit()
if connection.is_connected():
    db_Info = connection.get_server_info()
    cursor = connection.cursor(buffered=True)
    cursor.execute("select database();")
    record = cursor.fetchone()
    #print("You're connected to database: ", record) 
# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

log_messages['File created'] = output_filename
log_json_message()

input_file = open(input_filename, 'r')
# check for null values in input
input_rec = csv.DictReader((line.replace('\0','') for line in input_file), delimiter=',')
for row in input_rec:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLTXT00P_Field_format),3):
            output_record[DLTXT00P_Field_format[x]] = ''
            
        for col in DLTXT00P_field_map.keys():
            # move data to output column
            output_record[col] = row[DLTXT00P_field_map[col]]
            
        # special mapping and exclusion
        # For those items that don't have valid/unique ISBN13s,
        # use the Elan Product ID as the Item # and leave the Alternate Item blank
        if not output_record['TXTKEY']:
            output_record['TXTKEY'] = row['Elan Product ID']
        output_record['TXTKEY'] = output_record['TXTKEY'].strip(string.punctuation)
        # skip if TXTXT is blank
        if not output_record['TXTXT']:
            skip_record = True
            skip_count += 1
        # check all fields
        if not skip_record:
            DLTXT00P_validate_fields(output_record)
            
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['TXTKEY']
                log_messages['Status'] = 'record skipped'           
                log_json_message()
                skip_count += 1
                #print(output_record)
                #loggily_json_message(log_messages)
            else:
                values = output_record.values()
                database_insert(output_record)
                # don't write to output file if database insert failed
                if not skip_record:
                    csvwriter.writerow(values)
                    write_count += 1
                else:
                    skip_count += 1
        else:
            skip_count += 1

# close database connection
if connection.is_connected():
     connection.close()
     #print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Skipped']= skip_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= inserted_count
log_json_message()
sys.exit()