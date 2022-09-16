### DLCMA00P – Contact master file. Contains personal information about contact person.
### NOTE: Book Contacts-Contacts Master.csv and Individual Contacts Using Gen Comp_Indiv Names.csv
###  are inserted into the DB as incomplete records
###   Individual Contact Addresses using Gen CompInd Addresses is used to update specific fields in the database table.
###  DLCMA00P_extractor extracts the data to a tsv file.
import json
import csv
import datetime, time
import os, sys
import requests
import regex
from cerberus import Validator
import phonenumbers
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
input_filename_1 = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/Book Contacts-Contacts Master.csv'
input_filename_2 = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/Individual Contacts Using Gen CompIndiv Names.csv'
input_filename_3 = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/Individual Contact Addresses using Gen CompInd Addresses.csv'
# output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCMA00P/DLCMA00P-'+ datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False
update_record = True

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
# counters
line_count = 0
update_count = 0
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
    global insert_count, skip_record, cursor, connection, llmigration_table
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    # fix for utf-8 keys
    columns = columns.replace('\ufeff','')
    try:
        value_list = list(insert_record.values())
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, value_list)
        connection.commit()
        insert_count += 1
    except mysql.connector.DatabaseError as error:
        #if not 'Duplicate' in error:
        #    log_messages['MySQL_insert'] = str(error)
        #    log_json_message(log_messages)
        skip_record = True
        
def DLCMA00P_validate_fields(record):
    global skip_record
    # field specific mapping
    record['D1FMNM'] = record['D1FMNM'][0:19]
    record['D1MDNM'] = record['D1MDNM'][0:19]
    record['D1GVNM'] = record['D1GVNM'][0:19]
    record['D1FNM'] = record['D1FNM'][0:59]
    record['D1EMLA'] = record['D1EMLA'][0:49]
    # verify phone number formats
    if not record['D4ADRL6']:
        country = 'US'
    else:
        country = record['D4ADRL6']
    if len(record['D1PHN1']):
        try:
            parsed_phone = phonenumbers.parse(record['D1PHN1'], country)
            phonenumbers.is_valid_number(parsed_phone)
            record['D1PHN1'] = record['D1PHN1'].lstrip('-')
            record['D1PHN1'] = record['D1PHN1'][0:19]
        except:
            log_messages['invalid phone number redacted'] = record['D1PHN1']
            record['D1PHN1'] = ''
            log_json_message(log_messages)
    if len(record['D1PHN2']):
        try:
            parsed_phone = phonenumbers.parse(record['D1PHN2'], country)
            phonenumbers.is_valid_number(parsed_phone)
            record['D1PHN2'] = record['D1PHN2'].lstrip('-')
            record['D1PHN2'] = record['D1PHN2'][0:19]
        except:
            log_messages['invalid phone number redacted'] = record['D1PHN2']
            record['D1PHN2'] = ''
            log_json_message(log_messages)
    record['CECT'] = record['CECT'][0:2]
    # fix address lines from Excel formula residue
    if record['D4ADRL0'] == '0':
        record['D4ADRL0'] = ''
    else:
        record['D4ADRL0'] = record['D4ADRL0'].upper()
    if record['D4ADRL1'] == '0':
        record['D4ADRL1'] = ''    
    else:
        record['D4ADRL1'] = record['D4ADRL1'].upper()
    if record['D4ADRL2'] == '0':
        record['D4ADRL2'] = ''
    else:
        record['D4ADRL2'] = record['D4ADRL2'].upper()
    if record['D4ADRL3'] == '0':
        record['D4ADRL3'] = ''
    else:
        record['D4ADRL3'] = record['D4ADRL3'].upper()
    if record['D4ADRL4'] == '0':
        record['D4ADRL4'] = ''
    else:
        record['D4ADRL4'] = record['D4ADRL4'].upper()
    if record['D4ADRL5'] == '0':
        record['D4ADRL5'] = ''
    else:
        record['D4ADRL5'] = record['D4ADRL5'].upper()
    if record['D4ADRL6'] == '0':
        record['D4ADRL6'] = ''  
    else:
        record['D4ADRL6'] = record['D4ADRL6'].upper()
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
    cursor = connection.cursor(buffered=True)
    cursor.execute("select database();")
    record = cursor.fetchone()
    # print("You're connected to database: ", record) 


# Process input file 3
input_file = open(input_filename_3, 'r')
# check for null values in input
input_rec = csv.DictReader((line.replace('\0','') for line in input_file), delimiter=',')
for row in input_rec:
    # transform output record to field specifications
    skip_record = False
    log_messages = {}
    output_record = {}
    line_count += 1
    
    # initialize output_record keys
    for x in range(0, len(DLCMA00P_Field_format),3):
        output_record[DLCMA00P_Field_format[x]] = ''
    
    for col in field_map_compind_addresses:
        # move data to output column
        output_record[col] = row[field_map_compind_addresses[col]]
        
    # phone number assembly
    output_record['D1PHN1'] = (row['Telephone Area'] + ' ' + row['Telephone Number']).strip()
    output_record['D1PHN1'] = output_record['D1PHN1'].replace('\n\u00017','')
    output_record['D1PHN2'] = (row['Fax Area'] + ' ' + row['Fax Number']).strip()
    output_record['D1PHN2'] = output_record['D1PHN2'].replace('\n\u00017','')
    
    # Registration and Change Timestamps and users
    if output_record['D1CRTZ']:
        output_record['D1CRTZ'] =  datetime.datetime.strptime(output_record['D1CRTZ'], "%b %d, %Y").strftime("%Y-%m-%d")
    if output_record['D1CHGZ']:
        output_record['D1CHGZ'] =  datetime.datetime.strptime(output_record['D1CHGZ'], "%b %d, %Y").strftime("%Y-%m-%d")
    
    # check Do Not Use indicator
    if output_record['D4STS'] == 'Y':
        skip_record = True

    # retrieve the existing contacts master record
    if not skip_record:
        item_key = [output_record['D1SEQDM']]
        field_list = ['D1CRTZ','D1CRTU','D1CHGZ','D1CHGU','D4ADRL0','D4ADRL1','D4ADRL2','D4ADRL3','D4ADRL4','D4ADRL5','D4ADRL6']
        try:
            qry = 'SELECT D1CRTZ,D1CRTU,D1CHGZ,D1CHGU,D4ADRL0,D4ADRL1,D4ADRL2,D4ADRL3,D4ADRL4,D4ADRL5,D4ADRL6 FROM contact_master WHERE D1SEQDM = %s'
            cursor.execute(qry, item_key)
            connection.commit()
            contacts_rec = cursor.fetchone()
            update_record = False
            if contacts_rec:
                for x in range(len(field_list)):
                    # check field length
                    if len(output_record[field_list[x]]) > DLCMA00P_validator_schema[field_list[x]]['maxlength']:
                        str_length = DLCMA00P_validator_schema[field_list[x]]['maxlength'] - 1
                        output_record[field_list[x]] = output_record[field_list[x]][0:str_length]
                        update_record = True
                    # only update if field is empty
                    if contacts_rec[x] and (not output_record[field_list[x]]):
                        str_length = DLCMA00P_validator_schema[field_list[x]]['maxlength'] - 1
                        output_record[field_list[x]] = contacts_rec[x][0:str_length]
                        update_record = True
            else:
                skip_record = True
        except mysql.connector.DatabaseError as error:
            log_messages['MySQL_query'] = str(error)
            log_messages['indiv_contacts record not found'] = item_key
            log_json_message(log_messages)
    
    #check all fields
    if not skip_record:
        DLCMA00P_validate_fields(output_record)
    if not skip_record:
        # validate output record to specification
        if not v.validate(output_record):
            log_messages= v.errors
            log_messages['Status'] = 'record update skipped'           
            log_json_message(log_messages)
            loggily_json_message(log_messages)
            skip_record = True
   
    # update the contacts master record
    if not skip_record and update_record:
        item_list = [output_record['D1CRTZ'],output_record['D1CRTU'],output_record['D1CHGZ'],output_record['D1CHGU'],output_record['D4ADRL0'],output_record['D4ADRL1'],output_record['D4ADRL2'],output_record['D4ADRL3'],output_record['D4ADRL4'],output_record['D4ADRL5'],output_record['D4ADRL6'],output_record['D1SEQDM']]
        try:
            qry = 'UPDATE contact_master SET D1CRTZ = %s,D1CRTU = %s,D1CHGZ = %s,D1CHGU = %s,D4ADRL0 = %s,D4ADRL1 = %s,D4ADRL2 = %s,D4ADRL3 = %s,D4ADRL4 = %s,D4ADRL5 = %s,D4ADRL6 = %s where D1SEQDM = %s';
            cursor.execute(qry, item_list)
            connection.commit()
            contacts_rec = cursor.fetchone()
        except mysql.connector.DatabaseError as error:
            log_messages['MySQL_query'] = str(error)
            log_messages['indiv_contacts record not found'] = output_record['D1SEQDM']
            log_json_message(log_messages)
        update_count += 1
        
input_file.close()            
# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    #print("MySQL connection is closed")            


log_messages['Records Processed']= line_count
log_messages['Records Written to database']= insert_count
log_messages['Records updated']= update_count
log_json_message(log_messages)
sys.exit()