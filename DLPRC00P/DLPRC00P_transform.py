import json
import csv
import datetime, time
import string
import os, sys
import requests
import regex
from cerberus import Validator
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLPRC00P_map import *
from DLPRC00P_format import *
# Globals
DLPRC00P_encoding = {'I9I': 'ascii','I9PRCD': 'ascii','I9EXCD': 'ascii','I9TOS': 'ascii','I9EFFD': 'ascii','I9EXPD': 'ascii','I9PFRM': 'ascii','I9RRP': 'ascii','I9PRC1': 'ascii','I9QTY2': 'ascii','I9PRC2': 'ascii','I9QTY3': 'ascii','I9PRC3': 'ascii','I9QTY4': 'ascii','I9PRC4': 'ascii','I9QTY5': 'ascii','I9PRC5': 'ascii','I9QTY6': 'ascii','I9PRC6': 'ascii','I9QTY7': 'ascii','I9PRC7': 'ascii','I9QTY8': 'ascii','I9PRC8': 'ascii','I9QTY9': 'ascii','I9PRC9': 'ascii','I9QTY10': 'ascii','I9PRC10': 'ascii'}
DLPRC00P_validator_schema = {'I9I': {'type': 'string','maxlength': 20,'required':True},'I9PRCD': {'type': 'string','maxlength': 2,'required':True},'I9EXCD': {'type': 'string','maxlength': 1},'I9TOS': {'type': 'string','maxlength': 2},'I9EFFD': {'type': 'string','maxlength': 10},'I9EXPD': {'type': 'string','maxlength': 10},'I9PFRM': {'type': 'string','maxlength': 1},'I9RRP': {'type': 'string','maxlength': 10},'I9PRC1': {'type': 'string','maxlength': 10},'I9QTY2': {'type': 'string','maxlength': 14},'I9PRC2': {'type': 'string','maxlength': 10},'I9QTY3': {'type': 'string','maxlength': 14},'I9PRC3': {'type': 'string','maxlength': 10},'I9QTY4': {'type': 'string','maxlength': 14},'I9PRC4': {'type': 'string','maxlength': 10},'I9QTY5': {'type': 'string','maxlength': 14},'I9PRC5': {'type': 'string','maxlength': 10},'I9QTY6': {'type': 'string','maxlength': 14},'I9PRC6': {'type': 'string','maxlength': 10},'I9QTY7': {'type': 'string','maxlength': 14},'I9PRC7': {'type': 'string','maxlength': 10},'I9QTY8': {'type': 'string','maxlength': 14},'I9PRC8': {'type': 'string','maxlength': 10},'I9QTY9': {'type': 'string','maxlength': 14},'I9PRC9': {'type': 'string','maxlength': 10},'I9QTY10': {'type': 'string','maxlength': 14},'I9PRC10': {'type': 'string','maxlength': 10}}
DLPRC00P_record = DLPRC00P_encoding.keys()

log_messages={}
llmigration_table= 'price_file'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLPRC00P/DLPRC00P-220729.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLPRC00P/DLPRC00P-220729.tsv'
skip_record = False
previous_day = datetime.timedelta(1)

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
    
def DLPRC00P_validate_fields(record, skip_record):
    # field specific mapping
    log_messages['I9I'] = record['I9I']
    
    #globals
    global previous_ISBN13
    global previous_date
    
    # field specifics
    # if effective date is null, change to origin date
    if not record['I9EFFD']:
        record['I9EFFD'] = '0000-01-01'
    if not record['I9EXPD']:
        record['I9EXPD'] = '9999-12-31'
    # if the ISBN is that same as the last record, change the expiry date to 1 day before the last record's history date
    save_date = record['I9EXPD']
    if previous_ISBN13 == record['I9I']:
        record['I9EXPD'] = datetime.datetime.strptime(previous_date, "%Y-%m-%d") - previous_day
        record['I9EXPD']  = record['I9EXPD'].strftime("%Y-%m-%d")
    else:
        record['I9EXPD'] = '99999999'
        previous_ISBN13 = record['I9I']
    previous_date = save_date
    # default currency to 'U' - USD
    record['I9EXCD'] = 'U'
    # enforce decimal precision
    if record['I9RRP']:
        record['I9RRP'] = f"{float(record['I9RRP']):.2f}"
    if record['I9PRC1']:
        record['I9PRC1'] = f"{float(record['I9PRC1']):.2f}"
    if record['I9QTY2']:
        record['I9QTY2'] = f"{float(record['I9QTY2']):.4f}"
    if record['I9PRC2']:
        record['I9PRC2'] = f"{float(record['I9PRC2']):.2f}"
    if record['I9QTY2']:
        record['I9QTY2'] = f"{float(record['I9QTY2']):.4f}"
    if record['I9PRC2']:
        record['I9PRC2'] = f"{float(record['I9PRC2']):.2f}"
    if record['I9QTY3']:
        record['I9QTY3'] = f"{float(record['I9QTY3']):.4f}"
    if record['I9PRC3']:
        record['I9PRC3'] = f"{float(record['I9PRC3']):.2f}"
    if record['I9QTY4']:
        record['I9QTY4'] = f"{float(record['I9QTY4']):.4f}"
    if record['I9PRC4']:
        record['I9PRC4'] = f"{float(record['I9PRC4']):.2f}"
    if record['I9QTY5']:
        record['I9QTY5'] = f"{float(record['I9QTY5']):.4f}"
    if record['I9PRC5']:
        record['I9PRC5'] = f"{float(record['I9PRC5']):.2f}"
    if record['I9QTY6']:
        record['I9QTY6'] = f"{float(record['I9QTY6']):.4f}"
    if record['I9PRC6']:
        record['I9PRC6'] = f"{float(record['I9PRC6']):.2f}"
    if record['I9QTY7']:
        record['I9QTY7'] = f"{float(record['I9QTY7']):.4f}"
    if record['I9PRC7']:
        record['I9PRC7'] = f"{float(record['I9PRC7']):.2f}"
    if record['I9QTY8']:
        record['I9QTY8'] = f"{float(record['I9QTY8']):.4f}"
    if record['I9PRC8']:
        record['I9PRC8'] = f"{float(record['I9PRC8']):.2f}"
    if record['I9QTY9']:
        record['I9QTY9'] = f"{float(record['I9QTY9']):.4f}"
    if record['I9PRC9']:
        record['I9PRC9'] = f"{float(record['I9PRC9']):.2f}"
    if record['I9QTY10']:
        record['I9QTY10'] = f"{float(record['I9QTY10']):.4f}"
    if record['I9PRC10']:
        record['I9PRC10'] = f"{float(record['I9PRC10']):.2f}"
    
    
### MAIN ###  
# field validator setup
v = Validator(DLPRC00P_validator_schema)
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

previous_ISBN13 = '999999999999'
previous_date = ''

with open(input_filename) as csv_file:
    price_rec = csv.DictReader(csv_file, delimiter=',')
    for row in price_rec:
        #line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        line_count += 1
        
        # initialize output_record keys
        for x in range(0, len(DLPRC00P_field_format),3):
            output_record[DLPRC00P_field_format[x]] = ''
            
        # strip commas from prices
        row['History Prices'] = row['History Prices'].replace(',', '')
        row['List Price'] = row['List Price'].replace(',', '')
        
        # strip extraneous characters from I9I, Product ISBN13
        row['Product ISBN13'] = row['Product ISBN13'].strip('., ')
        
        # convert dates to uniform format
        if row['History Dates']:
            row['History Dates'] = datetime.datetime.strptime(row['History Dates'], "%b %d %Y").strftime("%Y-%m-%d")
        if row['List Date']:
            row['List Date'] = datetime.datetime.strptime(row['List Date'], "%b %d %Y").strftime("%Y-%m-%d")
           
        for col in field_map.keys():
                # move data to output column
                output_record[col] = row[field_map[col]]
             
        #check all fields
        DLPRC00P_validate_fields(output_record, skip_record)
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['I9I']
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