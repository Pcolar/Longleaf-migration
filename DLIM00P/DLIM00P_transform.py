import json
import csv
import datetime, time
import os, sys
import requests
import regex, re
from cerberus import Validator
# import phonenumbers
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLIM00P_master_map import *
from DLIM00P_group_map import *
from DLIM00P_I1DIV_map import *
from DLIM00P_I1IDIS_map import *
from DLIM00P_I1PACK_map import *
from DLIM00P_I1ISTS_map import *
from DLIM00P_format import *
from DLIM00P_MINOR_DISC import *

# Globals
log_messages={}
DLIM00P_encoding ={'I1CLS':'ascii','I1DIV':'ascii','I1GRP':'ascii','I1CAT':'ascii','I1SCT':'ascii','I1I':'ascii','I1AI':'ascii','I1SUPI':'ascii','I1IDSC':'utf-8','I1IEDC':'utf-8','I1CMNT':'utf-8','I1STKR':'ascii','I1BRNC':'ascii','I1PRFC':'ascii','I1IDIS':'ascii','I1EFEX':'ascii','I1ORGN':'ascii','I1UOM':'ascii','I1PPER':'ascii','I1CPER':'ascii','I1PKG':'ascii','I1RLSN':'ascii','I1OSRD':'ascii','I1CSMC':'ascii','I1PACK':'ascii','I1ABC':'ascii','I1STKF':'ascii','I1ILCC':'ascii','I1PTWC':'ascii','I1LPF':'ascii','I1CFBF':'ascii','I1FRCHG':'ascii','I1ISTS':'ascii','I1BMTH':'ascii','I1EFCS':'ascii','I1EDDS':'ascii','I1OSPR':'ascii','I1DTYR':'ascii','I1TRFC':'ascii','I1IGLC':'ascii','I1CRTQ':'ascii','I1VOL':'ascii','I1VLWT':'ascii','I1WGHN':'ascii','I1WGHT':'ascii','I1LNG':'ascii','I1WDTH':'ascii','I1DPTH':'ascii','I1EXTV':'ascii','I1EXTM':'ascii','I1IBCD':'ascii','I1IWSC':'ascii','I1REGD':'ascii','I1REGU':'ascii','I1CHGZ':'ascii','I1CHGU':'ascii','I1LANG':'ascii','I1EDTT':'ascii','I1TXC1':'ascii','I1TXP1':'ascii','I1TXC2':'ascii','I1TXP2':'ascii','I1TXC3':'ascii','I1TXP3':'ascii','I1TXC4':'ascii','I1TXP4':'ascii','I1TXC5':'ascii','I1TXP5':'ascii'}
DLIM00P_validator_schema = {'I1CLS':{'type':'string','maxlength':3,'empty':True},'I1DIV':{'type':'string','maxlength':3,'empty':False},'I1GRP':{'type':'string','maxlength':3,'empty':False},'I1CAT':{'type':'string','maxlength':3,'empty':True},'I1SCT':{'type':'string','maxlength':3,'empty':True},'I1I':{'type':'string','maxlength':20,'required':True},'I1AI':{'type':'string','maxlength':20,'empty':True},'I1SUPI':{'type':'string','maxlength':20,'empty':True},'I1IDSC':{'type':'string','maxlength':60,'empty':True},'I1IEDC':{'type':'string','maxlength':60,'empty':True},'I1CMNT':{'type':'string','maxlength':40,'empty':True},'I1STKR':{'type':'string','maxlength':10,'empty':True},'I1BRNC':{'type':'string','maxlength':3,'empty':True},'I1PRFC':{'type':'string','maxlength':3,'empty':True},'I1IDIS':{'type':'string','maxlength':3,'empty':True},'I1EFEX':{'type':'string','maxlength':1,'empty':True},'I1ORGN':{'type':'string','maxlength':4,'empty':True},'I1UOM':{'type':'string','maxlength':3,'empty':True},'I1PPER':{'type':'string','maxlength':7,'empty':True},'I1CPER':{'type':'string','maxlength':7,'empty':True},'I1PKG':{'type':'string','maxlength':5,'empty':True},'I1RLSN':{'type':'string','maxlength':4,'empty':True},'I1OSRD':{'type':'string','maxlength':10,'empty':True},'I1CSMC':{'type':'string','maxlength':4,'empty':True},'I1PACK':{'type':'string','maxlength':1,'empty':True},'I1ABC':{'type':'string','maxlength':1,'empty':True},'I1STKF':{'type':'string','maxlength':1,'empty':True},'I1ILCC':{'type':'string','maxlength':3,'empty':True},'I1PTWC':{'type':'string','maxlength':3,'empty':True},'I1LPF':{'type':'string','maxlength':1,'empty':True},'I1CFBF':{'type':'string','maxlength':1,'empty':True},'I1FRCHG':{'type':'string','maxlength':1,'empty':True},'I1ISTS':{'type':'string','maxlength':1,'empty':True},'I1BMTH':{'type':'string','maxlength':3,'empty':True},'I1EFCS':{'type':'string','maxlength':15,'empty':True},'I1EDDS':{'type':'string','maxlength':5,'empty':True},'I1OSPR':{'type':'string','maxlength':13,'empty':True},'I1DTYR':{'type':'string','maxlength':5,'empty':True},'I1TRFC':{'type':'string','maxlength':20,'empty':True},'I1IGLC':{'type':'string','maxlength':10,'empty':True},'I1CRTQ':{'type':'string','maxlength':5,'empty':True},'I1VOL':{'type':'string','maxlength':9,'empty':True},'I1VLWT':{'type':'string','maxlength':9,'empty':True},'I1WGHN':{'type':'string','maxlength':9,'empty':True},'I1WGHT':{'type':'string','maxlength':9,'empty':True},'I1LNG':{'type':'string','maxlength':7,'empty':True},'I1WDTH':{'type':'string','maxlength':7,'empty':True},'I1DPTH':{'type':'string','maxlength':7,'empty':True},'I1EXTV':{'type':'string','maxlength':9,'empty':True},'I1EXTM':{'type':'string','maxlength':2,'empty':True},'I1IBCD':{'type':'string','maxlength':20,'empty':True},'I1IWSC':{'type':'string','maxlength':3,'empty':True},'I1REGD':{'type':'string','maxlength':45,'empty':True},'I1REGU':{'type':'string','maxlength':10,'empty':True},'I1CHGZ':{'type':'string','maxlength':45,'empty':True},'I1CHGU':{'type':'string','maxlength':10,'empty':True},'I1LANG':{'type':'string','maxlength':3,'empty':True},'I1EDTT':{'type':'string','maxlength':3,'empty':True},'I1TXC1':{'type':'string','maxlength':3,'empty':True},'I1TXP1':{'type':'string','maxlength':6,'empty':True},'I1TXC2':{'type':'string','maxlength':3,'empty':True},'I1TXP2':{'type':'string','maxlength':6,'empty':True},'I1TXC3':{'type':'string','maxlength':3,'empty':True},'I1TXP3':{'type':'string','maxlength':6,'empty':True},'I1TXC4':{'type':'string','maxlength':3,'empty':True},'I1TXP4':{'type':'string','maxlength':6,'empty':True},'I1TXC5':{'type':'string','maxlength':3,'empty':True},'I1TXP5':{'type':'string','maxlength':6,'empty':True}}
DLIM00P_record = DLIM00P_encoding.keys()
llmigration_table='item_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIM00P/DLIM00P-220731.csv'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIM00P/DLIM00P-220803.tsv'
skip_record = False
pub_status_code = ''
last_sold_date = ''

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
redact_char = re.compile('[\r\n\t]*')
# counters
line_count = 0
write_count = 0
inserted_count = 0
skip_count = 0

def log_json_message(log_message):
    """print out  in json tagged log message format"""
    log_message['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    #log_message['program'] = os.path.basename(__file__)
    print(json.dumps(log_message))
    log_message={}
    
def loggily_json_message(log_message):
    """Push message to Loggily in json tagged log message format"""
    log_message['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    payload=json.dumps(log_message)
    response = requests.post(loggily_URI, data=payload)
    if response.status_code != 200:
        log_message['loggily error'] = response
    log_json_message(log_message)
    log_message={}
            
def database_insert(insert_record):
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    # fix utf-8 column names
    columns = columns.replace('\ufeff','')
    global inserted_count, pub_status_code, last_sold_date
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        inserted_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_insert'] = str(error)
        log_messages['Record ID'] = insert_record['I1I']
        log_messages['Last sold'] = last_sold_date
        log_messages['Pub status'] = pub_status_code
        log_json_message(log_messages)
        #sys.exit()
        
def DLIM00P_validate_fields(record):
    global skip_record, item_group
    # field specific mapping
    # length of I1CLS should be 3
    if len(record['I1CLS']) < 3:
        record['I1CLS'] = '{:0>3}'.format(record['I1CLS'])
    else:
        if len(record['I1CLS']) > 3:
            log_messages['I1CLS'] = record['I1CLS']
            log_messages['Record ID'] = record['I1I']
                  
    # map discount        
    if len(record['I1IDIS']) > 0:
        try:
            record['I1IDIS'] = I1IDIS_map[record['I1IDIS']]
        except KeyError:
            record['I1IDIS'] = ''
            
    # Set default values for I1LPF, I1CFBF, I1TXP1, I1TXP2, I1TXP3, I1TXP4, I1TXP5, I1UOM, I1CSMC, I1EDTT
    record['I1LPF'] = 'Y'
    record['I1CFBF'] = 'N'
    record['I1TXP1'] = '{:.2f}'.format(100.00)
    record['I1TXP2'] = '{:.2f}'.format(0.00)
    record['I1TXP3'] = '{:.2f}'.format(0.00)
    record['I1TXP4'] = '{:.2f}'.format(0.00)
    record['I1TXP5'] = '{:.2f}'.format(0.00)
    record['I1UOM'] = 'EA'
    record['I1CSMC'] = 'FIFO'
    record['I1PPER'] = '1'
    record['I1CPER'] = '1'
    record['I1EDTT'] = ''
    
    if len(record['I1OSRD']) == 0:
        record['I1OSRD'] = '0001-01-01'
    
    # map I1FRCHG
    if len(record['I1FRCHG']) > 0 and record['I1FRCHG'] == 'N':
        record['I1FRCHG'] = 'Y'
    else:
        record['I1FRCHG'] = 'N'
    
    # map I1CRTQ
    if len(record['I1CRTQ']) > 0 and int(record['I1CRTQ']) > 0:
        record['I1CRTQ'] = record['I1CRTQ'].strip()
    else:
        record['I1CRTQ'] = '1'
    
    # map I1PACK
    try:
        record['I1PACK'] = I1PACK_map[record['I1PACK']]
    except KeyError:
        record['I1PACK'] = 'N'
        
    # map I1ISTS
    try:
        record['I1ISTS'] = DLIM00P_I1ISTS_map[record['I1ISTS']]    
    except KeyError:
        log_messages['I1ISTS map error'] = record['I1ISTS']
        record['I1ISTS'] = ''
    # Registration and Change Timestamps
    if record['I1CHGZ']:
        record['I1CHGZ'] =  datetime.datetime.strptime(record['I1CHGZ'], "%b %d, %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
    if record['I1REGD']:
        record['I1REGD'] =  datetime.datetime.strptime(record['I1REGD'], "%b %d, %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        record['I1REGD'] = record['I1CHGZ']
    if not record['I1REGU']:
        record['I1REGU'] = record['I1CHGU']
    
    # enforce numeric formatting
    if record['I1EFCS']:
        record['I1EFCS'] = '{:.4f}'.format(float(record['I1EFCS']))
    if record['I1EDDS']:
        record['I1EDDS'] = '{:.2f}'.format(float(record['I1EDDS']))
    if record['I1OSPR']:
        record['I1OSPR'] = '{:.2f}'.format(float(record['I1OSPR']))
    if record['I1DTYR']:
        record['I1DTYR'] = '{:.2f}'.format(float(record['I1DTYR']))
    if record['I1VOL']:
        record['I1VOL'] = '{:.5f}'.format(float(record['I1VOL']))
    if record['I1VLWT']:
        record['I1VLWT'] = '{:.5f}'.format(float(record['I1VLWT']))
    if record['I1WGHN']:
        record['I1WGHN'] = '{:.5f}'.format(float(record['I1WGHN']))
    if record['I1WGHT']:
        record['I1WGHT'] = '{:.5f}'.format(float(record['I1WGHT']))
    if record['I1EXTV']:
        record['I1EXTV'] = '{:.2f}'.format(float(record['I1EXTV']))

    # sanitize to remove tabs, etc, truncate the item description (I1IDSC), extended description (I1IEDC),comment (I1CMNT)
    record['I1IDSC'] = re.sub(redact_char, "", record['I1IDSC'])[0:59]
    record['I1IEDC'] = re.sub(redact_char, "", record['I1IEDC'])[0:59]
    record['I1CMNT'] = re.sub(redact_char, "", record['I1CMNT'])[0:39]
    # truncate I1CHGU and I1REGU
    if len(record['I1CHGU']) > 10:
        record['I1CHGU'] = record['I1CHGU'][0:9]
    if len(record['I1REGU']) > 10:
        record['I1REGU'] = record['I1REGU'][0:9]
    # validate fields
    for field_index in range(0, len(DLIM00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        #print(f' index:', field_index, 'key:', DLIM00P_Field_format[field_index], 'Type: ', DLIM00P_Field_format[field_type], 'Length: ', DLIM00P_Field_format[field_length])
        if record[DLIM00P_Field_format[field_index]]:
            #log_messages['field'] = DLIM00P_Field_format[field_index]
            if  len(record[DLIM00P_Field_format[field_index]]) >  int(DLIM00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLIM00P_Field_format[field_length]
                skip_record = True
            if DLIM00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLIM00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLIM00P_Field_format[field_index]]
                    skip_record = True
            if DLIM00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLIM00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLIM00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        loggily_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLIM00P_validator_schema)
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
        for x in range(0, len(DLIM00P_Field_format),3):
            output_record[DLIM00P_Field_format[x]] = ''
            
        for col in field_map.keys():
            # move data to output column
            output_record[col] = row[field_map[col]]
            
        # special mapping and exclusion
        if row['Company NO']:
            if int(row['Company NO']) == 2 or int(row['Company NO']) == 12:
                log_messages['record skipped - I1CLS'] = row['Company NO']
                loggily_json_message(log_messages)
                skip_record = True
            else:
                div_key = row['Company NO'] + row['Publisher ID']
                div_key = div_key.strip()
                div_key = div_key.lstrip('0')
                try:
                    output_record['I1DIV'] = I1DIV_map[div_key].zfill(3)
                except KeyError:
                    output_record['I1DIV'] = ''
                    log_messages['I1DIV'] = 'mapping error'
                    log_messages['Key'] = div_key
                    log_messages['Record ID'] = output_record['I1I']
                    log_json_message
        # For those items that don't have valid/unique ISBN13s,
        # use the Elan Product ID as the Item # and leave the Alternate Item blank
        if not output_record['I1I']:
            output_record['I1I'] = row['Elan Product ID']
            output_record['I1AI'] = ''
        else:
            # strip quotes, delimiters and whitespace from ISBN
            output_record['I1I'] = output_record['I1I'].strip(' -.''')
            
        # map I1GRP & I1PKG
        try:
            output_record['I1PKG'] = pkg_group[output_record['I1GRP']]
        except KeyError:
            log_messages['I1PKG map failed'] = output_record['I1GRP']
            output_record['I1PKG'] = ''
        try:
            output_record['I1GRP'] = item_group[output_record['I1GRP']]
        except KeyError:
            log_messages['I1GRP map failed'] = output_record['I1GRP']
            output_record['I1GRP'] = ''
        # skip record if primary key is blank
        if not output_record['I1I']:
            skip_record = True
            log_messages['I1I is empty'] = 'record skipped'
        # map I1CAT
        output_record['I1CAT'] = ''
        #try:
        #    record['I1CAT'] = DLIM00P_minor_disc[record['I1CAT']]
        #except KeyError:
            # truncate to 3 characters
        #    record['I1CAT'] = record['I1CAT'][0:3]
            # save for validation checks
        pub_status_code = row['I1STKR']
        last_sold_date = row['Last Sold Date']
        
        # map I1STKF
        # start with 'N' as a default
        output_record['I1STKF'] = 'N'
        #if row['I1GRP'] == 'O':
        #    output_record['I1STKF'] = 'Y'
        if row['Non Stock Flag']== 'Y':
            output_record['I1STKF'] = 'D'
        else:
            if row['Non Stock Flag']== 'N':
                output_record['I1STKF'] = 'Y'
            #if row['Non Stock Flag']== 'N' and row['User Field Value 3'] == 'L':
            #    output_record['I1STKF'] = 'N'
            #else:
            #    output_record['I1STKF'] = 'N'  
        #if (row['I1GRP'] == 'C' or row['I1GRP'] == 'P') and row['User Field Value 3'] != 'L':
        #    output_record['I1STKF'] = 'Y'
        
        # map I1STKR
        try:
            output_record['I1STKR'] = I1STKR_map[row['I1STKR']]
        except KeyError:
            log_messages['I1STKR map error'] = output_record['I1STKR']
            output_record['I1STKR'] = ''
            
        # check all fields
        if not skip_record:
            DLIM00P_validate_fields(output_record)
            
        if not skip_record:
            # validate output record to specification
            if not v.validate(output_record):
                log_messages= v.errors
                log_messages['Record ID'] = output_record['I1I']
                log_messages['Status'] = 'record skipped'           
                loggily_json_message(log_messages)
                #print(output_record)
                #loggily_json_message(log_messages)
            else:
                values = output_record.values()
                csvwriter.writerow(values)
                database_insert(output_record)
                write_count += 1
        else:
            skip_count += 1

# close database connection
if connection.is_connected():
     connection.close()
     print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Skipped']= skip_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= inserted_count
log_json_message(log_messages)
sys.exit()