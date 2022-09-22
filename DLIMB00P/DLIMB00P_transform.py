### DLIMB00P – Title detail information. Includes stock status, pub dates, etc.
###
from fileinput import close
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
# field mapper and formats
from DLIMB00P_map import *
from DLIMB00P_format import *
# include group mapping from item file
from DLIM00P_maps import *

# Globals
DLIMB00P_encoding = {'BJI':'ascii','BJICG':'ascii','BJSTKSTS':'ascii','BJRLSD':'ascii','BJCSED':'ascii','BJFSIF':'ascii','BJLNKI':'ascii','BJPR1I':'ascii','BJPR2I':'ascii','BJPR3I':'ascii'}
DLIMB00P_validator_schema = {'BJI': {'type': 'string','maxlength': 20,'required':True},'BJICG': {'type': 'string','maxlength': 3,'required':True},'BJSTKSTS': {'type': 'string','maxlength': 1},'BJRLSD': {'type': 'string','maxlength': 10},'BJCSED': {'type': 'string','maxlength': 10},'BJFSIF': {'type': 'string','maxlength': 1},'BJLNKI': {'type': 'string','maxlength': 20},'BJPR1I': {'type': 'string','maxlength': 20},'BJPR2I': {'type': 'string','maxlength': 20},'BJPR3I': {'type': 'string','maxlength': 20}}
DLIMB00P_record = DLIMB00P_encoding.keys()

log_messages={}
llmigration_table= 'title_file'
input_filename =  '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIMB00P/DLIM00P.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIMB00P/DLIMB00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
# counters
line_count = 0
write_count = 0
insert_count = 0
skip_count = 0
# date comparisons
date_limit = datetime.datetime(2017,1,1)


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
        if not 'Duplicate' in error:
            log_messages['MySQL_insert'] = str(error)
            log_json_message(log_messages)
        skip_record = True
        
def DLIMB00P_validate_fields(record):
    global skip_record, log_messages
    # field specific mapping
    #log_messages['BJI'] = record['BJI']    
    # field specifics
    # Flip Returnables value
    if record['BJFSIF'] == 'N':
        record['BJFSIF'] = 'Y'
    else:
        record['BJFSIF'] = 'N'
    # BJICG is blank
    record['BJICG'] = ''    
    # normalize dates
    if record['BJRLSD']:
        record['BJRLSD'] =  datetime.datetime.strptime(record['BJRLSD'], "%b %d, %Y").strftime("%Y-%m-%d")
    # if record['BJCSED']:
    #    record['BJCSED'] =  datetime.datetime.strptime(record['BJCSED'], "%b %d, %Y").strftime("%Y-%m-%d")
    #    if record['BJCSED'] < record['BJRLSD']:
    #        log_messages['Cease Date is less than Release Date'] = str(record['BJCSED']) + ':' + str(record['BJRLSD'])
    #        log_messages['Record ID'] = record['BJI']
    #        log_json_message(log_messages)
    #        record['BJCSED'] = '9999-12-31'
    record['BJCSED'] = '9999-12-31'
        

### MAIN ###  
# field validator setup
v = Validator(DLIMB00P_validator_schema)
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
    #cursor = connection.cursor()
    cursor.execute("select database();")
    record = cursor.fetchone()
    # print("You're connected to database: ", record) 

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

log_messages['File created'] = output_filename
log_json_message(log_messages)

input_file = open(input_filename, 'r')
# check for null values in input
input_rec = csv.DictReader((line.replace('\0','') for line in input_file), delimiter=',')
for row in input_rec:
    # transform output record to field specifications
    skip_record = False
    log_messages = {}
    output_record = {}
    line_count += 1
    # initialize output_record keys
    for x in range(0, len(DLIMB00P_field_format),3):
        output_record[DLIMB00P_field_format[x]] = ''

    for col in field_map.keys():
        # move data to output column
        output_record[col] = row[field_map[col]]
        # specific mappings
        # Item can be ISBN13, Alternate item, or Supplier Item Code
        if not row['I1I']:
            if row ['I1AI']:
                 output_record['BJI'] = row['I1AI']
            else:
                output_record['BJI'] = row['Elan Product ID']
        # normalize BJI
        output_record['BJI'] = output_record['BJI'].strip(' -.''')
        # map BJSTKSTS
        if (row['Pod Warehouse IDs'] == '07' or row['Pod Warehouse IDs'] == '7') and row['User Field Value 3'] == 'L':
            output_record['BJSTKSTS'] = 'L'
        else:
            try:
                output_record['BJSTKSTS'] = BJSTKSTS_map[row['I1STKR']]
            except KeyError:
                log_messages['BJKSTKSTS'] = row['I1STKR']
                log_messages['error'] = 'mapping failed'
                log_messages['Record ID'] = output_record['BJI']
                log_json_message
                log_messages = {}
                output_record['BJSTKSTS'] = ''
                
        # Don't import records with 1) a pub date before 1/1/2017, 2) a last purchase/sale date before 1/1/2017, and 3) an Elan Pub Status ID of D or O.
        # Treat blanks as pre- 1/1/17.
        if (row['I1STKR'] == 'O' or row['I1STKR'] == 'D'):
            pub_date = datetime.datetime(1900,1,1)
            sale_date = datetime.datetime(1900,1,1)
            if row['Publication Date']:
                pub_date = datetime.datetime.strptime(row['Publication Date'], "%b %d, %Y")
            if row['Last Sold Date']:
                sale_date = datetime.datetime.strptime(row['Last Sold Date'], "%m/%d/%Y")
            if pub_date < date_limit or sale_date < date_limit:
                skip_record = True
                
    #check all fields
    if not skip_record:
        DLIMB00P_validate_fields(output_record)
    # verify an item master record exists
    item_list = [output_record['BJI']]
    if not skip_record:
         # retrieve the corresponding item master
        try:
            qry = 'Select I1I from item_master where I1I = %s'
            cursor.execute(qry, item_list)
            connection.commit()
            item_master_rec = cursor.fetchone()
            if not item_master_rec:
                skip_record = True
                log_messages['item_master not found'] = output_record['BJI']
        except mysql.connector.DatabaseError as error:
            log_messages['MySQL_query'] = str(error)
            log_messages['item_master not found'] = output_record['BJI']
            skip_record = True       
    if not skip_record:
        # validate output record to specification
        if not v.validate(output_record):
            log_messages= v.errors
            log_messages['Record ID'] = output_record['BJI']
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

# close csv file
input_file.close()

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    # print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_messages['Records skipped'] = skip_count
log_json_message(log_messages)
sys.exit()            
