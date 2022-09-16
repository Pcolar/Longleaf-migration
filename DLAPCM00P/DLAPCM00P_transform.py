
### DLAPCM00P- A/P creditors master file.
import json
import csv
import datetime
import os, sys
import requests
import regex
from cerberus import Validator
import phonenumbers
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLAPCM00P_map  import *
from DLAPCM00P_format import *

# Globals
log_messages={}
DLAPCM00P_encoding = {'A1CN': 'ascii','A1STS': 'ascii','A1CNM': 'utf-8','A1CAD0': 'utf-8','A1CAD1': 'utf-8','A1CAD2': 'utf-8','A1CAD3': 'utf-8','A1CAD4': 'utf-8','A1CAD5': 'ascii','A1CAD6': 'utf-8','A1DXNO': 'ascii','A1DXLOC': 'ascii','A1PNM': 'utf-8','A1PAD0': 'utf-8','A1PAD1': 'utf-8','A1PAD2': 'utf-8','A1PAD3': 'utf-8','A1PAD4': 'utf-8','A1PAD5': 'ascii','A1PAD6': 'utf-8','A1PHN': 'ascii','A1FAX': 'ascii','A1SAN': 'ascii','A1EMLA': 'ascii','A1DTTP': 'ascii','A1TFN': 'ascii','A1CONO': 'ascii','A1EXCD': 'ascii','A1APCC': 'ascii','A1DGLA': 'ascii','A1BNKA': 'ascii','A1BNKV': 'ascii','A1MISF': 'ascii','A1HOLD': 'ascii','A1SRFL': 'ascii','A1PYTC': 'ascii','A1DSCR': 'ascii','A1DTDS': 'utf-8','A1REGZ': 'ascii','A1REGU': 'ascii','A1CHGZ': 'ascii','A1CHGU': 'ascii'}
DLAPCM00P_validator_schema = {'A1CN':{'type': 'string','maxlength': 10,'required':True},'A1STS':{'type': 'string','maxlength': 1},'A1CNM':{'type': 'string','maxlength': 60},'A1CAD0':{'type': 'string','maxlength': 60},'A1CAD1':{'type': 'string','maxlength': 60},'A1CAD2':{'type': 'string','maxlength': 60},'A1CAD3':{'type': 'string','maxlength': 60},'A1CAD4':{'type': 'string','maxlength': 20},'A1CAD5':{'type': 'string','maxlength': 20},'A1CAD6':{'type': 'string','maxlength': 60},'A1DXNO':{'type': 'string','maxlength': 10},'A1DXLOC':{'type': 'string','maxlength': 20},'A1PNM':{'type': 'string','maxlength': 60},'A1PAD0':{'type': 'string','maxlength': 60},'A1PAD1':{'type': 'string','maxlength': 60},'A1PAD2':{'type': 'string','maxlength': 60},'A1PAD3':{'type': 'string','maxlength': 60},'A1PAD4':{'type': 'string','maxlength': 20},'A1PAD5':{'type': 'string','maxlength': 20},'A1PAD6':{'type': 'string','maxlength': 60},'A1PHN':{'type': 'string','maxlength': 20},'A1FAX':{'type': 'string','maxlength': 20},'A1SAN':{'type': 'string','maxlength': 20},'A1EMLA':{'type': 'string','maxlength': 120},'A1DTTP':{'type': 'string','maxlength': 3},'A1TFN':{'type': 'string','maxlength': 20},'A1CONO':{'type': 'string','maxlength': 2},'A1EXCD':{'type': 'string','maxlength': 1},'A1APCC':{'type': 'string','maxlength': 3},'A1DGLA':{'type': 'string','maxlength': 20},'A1BNKA':{'type': 'string','maxlength': 20},'A1BNKV':{'type': 'string','maxlength': 2},'A1MISF':{'type': 'string','maxlength': 1},'A1HOLD':{'type': 'string','maxlength': 1},'A1SRFL':{'type': 'string','maxlength': 1},'A1PYTC':{'type': 'string','maxlength': 2},'A1DSCR':{'type': 'string','maxlength': 6},'A1DTDS':{'type': 'string','maxlength': 20},'A1REGZ':{'type': 'string','maxlength': 45},'A1REGU':{'type': 'string','maxlength': 10},'A1CHGZ':{'type': 'string','maxlength': 45},'A1CHGU':{'type': 'string','maxlength': 10}}
DLAPCM00P_record = DLAPCM00P_encoding.keys()
llmigration_table='vendor_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLAPCM00P/AP Vendors - DLAPCM00P-220630.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLAPCM00P/DLAPCM00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
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
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    global insert_count
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
        
def DLAPCM00P_validate_fields(record, skip_record):
    # field specific mapping
    # length of C1CN should be 8
    record['A1CN'] = record['A1CN'].zfill(8)
    log_messages['A1CN'] = record['A1CN']
    
    # field specifics
    # Map Pay Status
    if record['A1STS']:
        record['A1STS'] = A1STS_map[record['A1STS']]
    #Map Vendor Type
    if record['A1APCC']:
        record['A1APCC'] = A1APCC_map[record['A1APCC']]
    # Map Payment Terms
    if record['A1PYTC']:
        record['A1PYTC'] = A1PYTC_map[record['A1PYTC']]
        
    # verify phone and fax numer formats according to country
    if len(record['A1PHN']) > 0:
        try:
            phone_number = phonenumbers.parse(record['A1PHN'], record['A1CAD6'])
            if not phonenumbers.is_valid_number(phone_number):
                log_messages['invalid phone number redacted'] = record['A1PHN']
                record['A1PHN'] = ''
                log_json_message(log_messages)
        except:
            log_messages['invalid phone number redacted'] = record['A1PHN']
            record['A1PHN'] = ''
            log_json_message(log_messages)
                
    if len(record['A1FAX']) > 0:
        try:
            fax_number = phonenumbers.parse(record['A1FAX'], record['A1CAD6']) 
            if not phonenumbers.is_valid_number(fax_number):
                log_messages['invalid fax number redacted'] = record['A1FAX']
                record['A1FAX'] = ''
                log_json_message(log_messages)
        except:
            log_messages['invalid fax number redacted'] = record['A1FAX']
            record['A1FAX'] = ''
            log_json_message(log_messages)
            
    # Enforce format
    if record['A1DSCR']:
        record['A1DSCR'] = '{:.2f}'.record['A1DSCR']
            
    # Normalize Dates
    if record['A1REGZ']:
        timecalc = datetime.datetime.strptime(record['A1REGZ'], "%b %d, %Y").strftime("%Y-%m-%d")
        record['A1REGZ'] = timecalc + "-04.00.00 " + timecalc + "-00.00.00EDT"
    if record['A1CHGZ']:
        timecalc = datetime.datetime.strptime(record['A1CHGZ'], "%b %d, %Y").strftime("%Y-%m-%d")
        record['A1CHGZ'] = timecalc + "-04.00.00 " + timecalc + "-00.00.00EDT"
        
    # Map Remittance Flag
    # set to default 
    record['A1SRFL'] = '2'
    record['A1CONO'] = '00'
    
    # Map Hold Flag
    if record['A1HOLD'] == 'H':
        record['A1HOLD'] = 'Y'
    else:
        record['A1HOLD'] = ''
    # MIS Account Flag default
    record['A1MISF'] = 'N'
    # Truncate long Company Name A1CNM
    if len(record['A1CNM']) > 60:
        record['A1CNM'] = record['A1CNM'][0:59]
    
    # validate fields
    for field_index in range(0, len(DLAPCM00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        # print(f' index:', field_index, 'key:', DLAPCM00P_Field_format[field_index], 'Type: ', DLAPCM00P_Field_format[field_type], 'Length: ', DLAPCM00P_Field_format[field_length])
        if record[DLAPCM00P_Field_format[field_index]]:
            log_messages['field'] = DLAPCM00P_Field_format[field_index]
            if  len(record[DLAPCM00P_Field_format[field_index]]) >  int(DLAPCM00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLAPCM00P_Field_format[field_length]
                skip_record = True
            if DLAPCM00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLAPCM00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLAPCM00P_Field_format[field_index]]
                    skip_record = True
            if DLAPCM00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLAPCM00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLAPCM00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLAPCM00P_validator_schema)
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
    # cursor = connection.cursor()
    cursor = connection.cursor(buffered=True)
    cursor.execute("select database();")
    record = cursor.fetchone()
    print("You're connected to database: ", record) 

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

log_messages['File created'] = output_filename
log_json_message(log_messages)

with open(input_filename) as csv_file:
    cust_master = csv.DictReader(csv_file, delimiter=',')
    for row in cust_master:
        line_count += 1
        # transform output record to field specifications
        skip_record = False
        log_messages = {}
        output_record = {}
        # initialize output_record keys
        for x in range(0, len(DLAPCM00P_Field_format),3):
            clean_key = DLAPCM00P_Field_format[x].replace('\ufeff','')
            output_record[clean_key] = ''

        for col in field_map.keys():
            # fix for utf-8 column names
            col = col.replace('\ufeff','')
            # move data to output column
            output_record[col] = row[field_map[col]]
            
        # field transformations
        if row['Telephone Area Code'] or row['Telephone Number']:
            output_record['A1PHN'] = row['Telephone Area Code'] + ' ' + row['Telephone Number']
            output_record['A1PHN'] = output_record['A1PHN'].strip()
        if row['Fax Area'] or row['Fax Number']:
            output_record['A1FAX'] = row['Fax Area'] + ' ' + row['Fax Number']
            output_record['A1FAX'] = output_record['A1FAX'].strip()
            
        #check all fields
        DLAPCM00P_validate_fields(output_record, skip_record)
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['A1CN']
                log_messages['Status'] = 'record skipped'           
                log_json_message(log_messages)
                #loggily_json_message(log_messages)
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