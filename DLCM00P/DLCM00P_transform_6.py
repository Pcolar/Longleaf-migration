### DLCM00P – Customer master file stores customers' names and addresses, as well as many other codes.
# script 6 - read records from the database and move them to a tsv file
import json
import csv
import datetime, time
import os, sys
import requests
import regex, re
from cerberus import Validator
import phonenumbers
import mysql.connector

# hidden parameters
from llsecrets import *
# field mapper and formats
from DLCM00P_map  import *
from DLCM00P_format import *

# Globals
log_messages={}
DLCM00P_encoding = {'C1CN': 'ascii','C1REGN': 'utf-8','C1PIDN': 'utf-8','C1PCNM': 'utf-8','C1PAD0': 'utf-8','C1PAD1': 'utf-8','C1PAD2': 'utf-8','C1PAD3': 'utf-8','C1PAD4': 'utf-8','C1PAD5': 'ascii','C1PAD6': 'utf-8','C1PDLVN': 'ascii','C1DXNO': 'ascii','C1DXLOC': 'ascii','C1PHN': 'ascii','C1FAX': 'ascii','C1CSTS': 'ascii','C1CONO': 'ascii','C1BR': 'ascii','C1RGN': 'ascii','C1SLRP': 'ascii','C1CDIS': 'ascii','C1PRCD': 'ascii','C1XINV': 'ascii', 'C1XNVF': 'ascii', 'C1CTXC': 'ascii', 'C1TXNO': 'ascii', 'C1PYTC': 'ascii', 'C1ILIC': 'ascii', 'C1SAN': 'ascii', 'C1BN': 'ascii', 'C1CCLS': 'ascii', 'C1EXCD': 'ascii', 'C1CRLM': 'ascii', 'C1CIAC': 'ascii', 'C1IBR': 'ascii', 'C1WH': 'ascii', 'C1CAR': 'ascii', 'C1FAGC': 'ascii', 'C1RUN': 'ascii', 'C1DSTP': 'ascii', 'C1STTY': 'ascii', 'C1CMNT': 'utf-8', 'C1OIF': 'ascii', 'C1DLVP': 'ascii', 'C1DLOP': 'ascii', 'C1CBOA': 'ascii', 'C1OCON': 'ascii', 'C1SBI': 'ascii', 'C1ONRF': 'ascii', 'C1CUTO': 'ascii', 'C1MIF': 'ascii', 'C1FSCF': 'ascii', 'C1CIEC': 'ascii', 'C1INCA': 'ascii', 'C1DFCF': 'ascii', 'C1AFCG': 'ascii', 'C1CFRT': 'ascii', 'C1CL1': 'ascii', 'C1CL2': 'ascii', 'C1CL3': 'ascii', 'C1CL4': 'ascii', 'C1CL5': 'ascii', 'C1MJS': 'ascii', 'C1STN': 'ascii', 'C1BOCR': 'ascii', 'C1ECR': 'ascii', 'C1STMT': 'ascii', 'C1RLSP': 'ascii', 'C1CTO': 'ascii', 'C1CASC': 'utf-8', 'C1REGZ': 'ascii', 'C1REGU': 'ascii', 'C1CHGZ': 'ascii', 'C1CHGU': 'ascii'}
DLCM00P_validator_schema = {'C1CN':{'type':'string','required':True,'maxlength':8},'C1REGN':{'type':'string','empty':True,'maxlength':60},'C1PDN':{'type':'string','empty':True,'maxlength':20},'C1PCNM':{'type':'string','empty':True,'maxlength':60},'C1PAD0':{'type':'string','empty':True,'maxlength':60},'C1PAD1':{'type':'string','empty':True,'maxlength':60},'C1PAD2':{'type':'string','empty':True,'maxlength':60},'C1PAD3':{'type':'string','empty':True,'maxlength':60},'C1PAD4':{'type':'string','empty':True,'maxlength':60},'C1PAD5':{'type':'string','empty':True,'maxlength':60},'C1PAD6':{'type':'string','empty':True,'maxlength':60},'C1PDLVN':{'type':'string','empty':True,'max':999,'min':0},'C1DXNO':{'type':'string','empty':True,'maxlength':10},'C1DXLOC':{'type':'string','empty':True,'maxlength':20},'C1PHN':{'type':'string','empty':True,'maxlength':20},'C1FAX':{'type':'string','empty':True,'maxlength':20},'C1CSTS':{'type':'string','empty':False,'maxlength':1,'allowed':['A','S','C']},'C1CONO':{'type':'string','empty':True,'maxlength':2},'C1BR':{'type':'string','empty':True,'maxlength':2},'C1RGN':{'type':'string','empty':True,'maxlength':2},'C1SLRP':{'type':'string','empty':True,'maxlength':6},'C1CDIS':{'type':'string','empty':True,'maxlength':3},'C1PRCD':{'type':'string','empty':True,'maxlength':2},'C1XINV':{'type':'string','empty':True,'max':999,'min':0},'C1XNVF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CTXC':{'type':'string','empty':True,'maxlength':3},'C1TXNO':{'type':'string','empty':True,'maxlength':15},'C1PYTC':{'type':'string','empty':True,'maxlength':2},'C1ILIC':{'type':'string','empty':True,'maxlength':20},'C1SAN':{'type':'string','empty':True,'maxlength':20},'C1BN':{'type':'string','empty':True,'maxlength':10},'C1CCLS':{'type':'string','empty':True,'maxlength':3},'C1EXCD':{'type':'string','empty':True,'maxlength':1},'C1CRLM':{'type':'string','empty':True},'C1CIAC':{'type':'string','empty':True},'C1CIBR':{'type':'string','empty':True,'maxlength':2},'C1WH':{'type':'string','empty':True,'maxlength':2},'C1CAR':{'type':'string','empty':True,'maxlength':2},'C1FAGC':{'type':'string','empty':True,'maxlength':2},'C1RUN':{'type':'string','empty':True,'maxlength':3},'C1DSTP':{'type':'string','empty':True,'maxlength':3},'C1STTY':{'type':'string','empty':True,'maxlength':1},'C1CMNT':{'type':'string','empty':True,'maxlength':40},'C1OIF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1DLVP':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1DLOP':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CBOA':{'type':'string','empty':True,'maxlength':1},'C1OCON':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1SBI':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1ONRF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CUTO':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1MIF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1FSCF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CIEC':{'type':'string','empty':True,'maxlength':1},'C1INCA':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1DFCF':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1AFCG':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CFRT':{'type':'string','empty':True,'maxlength':1},'C1CL1':{'type':'string','empty':True,'maxlength':3},'C1CL2':{'type':'string','empty':True,'maxlength':3},'C1CL3':{'type':'string','empty':True,'maxlength':3},'C1CL4':{'type':'string','empty':True,'maxlength':3},'C1CL5':{'type':'string','empty':True,'maxlength':3},'C1MJS':{'type':'string','empty':True,'maxlength':3},'C1STN':{'type':'string','empty':True,'maxlength':6},'C1BOCR':{'type':'string','empty':True,'maxlength':1},'C1ECR':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1STMT':{'type':'string','empty':True,'maxlength':1},'C1RLSP':{'type':'string','empty':True,'maxlength':6},'C1CTO':{'type':'string','empty':True,'maxlength':1,'allowed':['Y','N']},'C1CASC':{'type':'string','empty':True,'maxlength':20},'C1REGZ':{'type':'string','empty':True,'maxlength':45},'C1REGU':{'type':'string','empty':True,'maxlength':10},'C1CHGZ':{'type':'string','empty':True,'maxlength':45},'C1CHGU':{'type':'string','empty':True,'maxlength':10}}
DLCM00P_record = DLCM00P_encoding.keys()
llmigration_table='customer_master'
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLCM00P/DLCM00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
redact_char = re.compile('[\r\n\t]*')
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
            
def DLCM00P_validate_fields(record):
    global skip_record
    # field specific mapping
    # verify phone and fax numer formats
    if len(record['C1PHN']):
        try:
            parsed_phone = phonenumbers.parse(record['C1PHN'], record['C1PAD6'])
            phonenumbers.is_valid_number(parsed_phone)
        except:
            log_messages['invalid phone number redacted'] = record['C1PHN']
            record['C1PHN'] = ''
            log_json_message(log_messages)

    if len(record['C1FAX']):
        try:
            parsed_phone = phonenumbers.parse(record['C1FAX'], record['C1PAD6'])
            phonenumbers.is_valid_number(parsed_phone)
        except:
            log_messages['invalid fax number redacted'] = record['C1FAX']
            record['C1FAX'] = ''
            log_json_message(log_messages)
    # limit C1CMNT to 40 chars and redact non-printables
    record['C1CMNT'] = re.sub(redact_char, "", record['C1CMNT'])[0:39]
    # Map C1CTO
    if record['C1CTO'] == 'N' or record['C1CTO'] == 'F':
        record['C1CTO'] = 'N'
    else:
        if record['C1CTO'] == 'F1':
         record['C1CTO'] = 'Y'   
    # validate fields
    for field_index in range(0, len(DLCM00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        # print(f' index:', field_index, 'key:', DLCM00P_Field_format[field_index], 'Type: ', DLCM00P_Field_format[field_type], 'Length: ', DLCM00P_Field_format[field_length])
        if record[DLCM00P_Field_format[field_index]]:
            log_messages['field'] = DLCM00P_Field_format[field_index]
            if  len(record[DLCM00P_Field_format[field_index]]) >  int(DLCM00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLCM00P_Field_format[field_length]
                skip_record = True
            if DLCM00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLCM00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLCM00P_Field_format[field_index]]
                    skip_record = True
            if DLCM00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLCM00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLCM00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
    
            
### MAIN ###  
# field validator setup
v = Validator(DLCM00P_validator_schema)
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
    #print("You're connected to database: ", record) 

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')
log_messages['File created'] = output_filename
log_json_message(log_messages)

# retrieve customer Master records from the database
try:
    qry = 'Select * from customer_master'
    cursor.execute(qry)
    connection.commit()
    cust_master_rec = cursor.fetchall()
except mysql.connector.DatabaseError as error:
    skip_record = True
    log_messages['MySQL_query'] = str(error)
    log_json_message(log_messages)   
for row in cust_master_rec:
    line_count += 1
    # transform output record to field specifications
    skip_record = False
    log_messages = {}
    output_record = {}
    # initialize output_record keys
    for x in range(0, len(DLCM00P_Field_format),3):
        #clean_key = DLCM00P_Field_format[x].replace('\ufeff','')
        output_record[DLCM00P_Field_format[x]] = ''
    column = 0
    for col in DLCM00P_record:
        # move data to output column
        output_record[col] = row[column]
        column += 1
        
    # Field specific mappings
    # length of C1CN should be 8
    output_record['C1CN'] = output_record['C1CN'].zfill(8)
    #if not skip_record:
    #    DLCM00P_validate_fields(output_record)
    # output record    
    if not skip_record:
        # validate output record to specification
        if not v.validate(output_record):
            log_messages= v.errors
            log_messages['Record ID'] = output_record['C1CN']
            log_messages['Status'] = 'record skipped'           
            log_json_message(log_messages)
            skipped_count += 1
            #loggily_json_message(log_messages)
        else:
            values = output_record.values()
            if not skip_record:
                csvwriter.writerow(values)        
                write_count += 1
    else:
        skipped_count += 1

# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    #print("MySQL connection is closed") 
# close output file 
output_file.close()          

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()