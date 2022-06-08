import json
import csv
import datetime, time
import os, sys
import requests
import mysql.connector

# hidden parameters
from secrets import *

# field mapper and formats
from item_master_map  import *
from item_group_map import *
from DLIM00P_format import *

# Globals
log_messages={}
DLIM00P_encoding ={'I1CLS':'ascii','I1DIV':'ascii','I1GRP':'ascii','I1CAT':'ascii','I1SCT':'ascii','I1I':'ascii','I1AI':'ascii','I1SUPI':'ascii','I1IDSC':'utf-8','I1IEDC':'utf-8','I1CMNT':'utf-8','I1STKR':'ascii','I1BRNC':'ascii','I1PRFC':'ascii','I1IDIS':'ascii','I1EFEX':'ascii','I1ORGN':'ascii','I1UOM':'ascii','I1PPER':'ascii','I1CPER':'ascii','I1PKG':'ascii','I1RLSN':'ascii','I1OSRD':'ascii','I1CSMC':'ascii','I1PACK':'ascii','I1ABC':'ascii','I1STKF':'ascii','I1ILCC':'ascii','I1PTWC':'ascii','I1LPF':'ascii','I1CFBF':'ascii','I1FRCHG':'ascii','I1ISTS':'ascii','I1BMTH':'ascii','I1EFCS':'ascii','I1EDDS':'ascii','I1OSPR':'ascii','I1DTYR':'ascii','I1TRFC':'ascii','I1IGLC':'ascii','I1CRTQ':'ascii','I1VOL':'ascii','I1VLWT':'ascii','I1WGHN':'ascii','I1WGHT':'ascii','I1LNG':'ascii','I1WDTH':'ascii','I1DPTH':'ascii','I1EXTV':'ascii','I1EXTM':'ascii','I1IBCD':'ascii','I1IWSC':'ascii','I1REGD':'ascii','I1REGU':'ascii','I1CHGZ':'ascii','I1CHGU':'ascii','I1LANG':'ascii','I1EDTT':'ascii','I1TXC1':'ascii','I1TXP1':'ascii','I1TXC2':'ascii','I1TXP2':'ascii','I1TXC3':'ascii','I1TXP3':'ascii','I1TXC4':'ascii','I1TXP4':'ascii','I1TXC5':'ascii','I1TXP5':'ascii'}
DLIM00P_validator_schema = {'I1CLS':{'type':'string','maxlength':3,'empty':True},'I1DIV':{'type':'string','maxlength':3,'empty':True},'I1GRP':{'type':'string','maxlength':3,'empty':True},'I1CAT':{'type':'string','maxlength':3,'empty':True},'I1SCT':{'type':'string','maxlength':3,'empty':True},'I1I':{'type':'string','maxlength':20,'required':True},'I1AI':{'type':'string','maxlength':20,'empty':True},'I1SUPI':{'type':'string','maxlength':20,'empty':True},'I1IDSC':{'type':'string','maxlength':60,'empty':True},'I1IEDC':{'type':'string','maxlength':60,'empty':True},'I1CMNT':{'type':'string','maxlength':40,'empty':True},'I1STKR':{'type':'string','maxlength':10,'empty':True},'I1BRNC':{'type':'string','maxlength':3,'empty':True},'I1PRFC':{'type':'string','maxlength':3,'empty':True},'I1IDIS':{'type':'string','maxlength':3,'empty':True},'I1EFEX':{'type':'string','maxlength':1,'empty':True},'I1ORGN':{'type':'string','maxlength':4,'empty':True},'I1UOM':{'type':'string','maxlength':3,'empty':True},'I1PPER':{'type':'string','maxlength':7,'empty':True},'I1CPER':{'type':'string','maxlength':7,'empty':True},'I1PKG':{'type':'string','maxlength':5,'empty':True},'I1RLSN':{'type':'string','maxlength':4,'empty':True},'I1OSRD':{'type':'string','maxlength':10,'empty':True},'I1CSMC':{'type':'string','maxlength':4,'empty':True},'I1PACK':{'type':'string','maxlength':1,'empty':True},'I1ABC':{'type':'string','maxlength':1,'empty':True},'I1STKF':{'type':'string','maxlength':1,'empty':True},'I1ILCC':{'type':'string','maxlength':3,'empty':True},'I1PTWC':{'type':'string','maxlength':3,'empty':True},'I1LPF':{'type':'string','maxlength':1,'empty':True},'I1CFBF':{'type':'string','maxlength':1,'empty':True},'I1FRCHG':{'type':'string','maxlength':1,'empty':True},'I1ISTS':{'type':'string','maxlength':1,'empty':True},'I1BMTH':{'type':'string','maxlength':3,'empty':True},'I1EFCS':{'type':'string','maxlength':15,'empty':True},'I1EDDS':{'type':'string','maxlength':5,'empty':True},'I1OSPR':{'type':'string','maxlength':13,'empty':True},'I1DTYR':{'type':'string','maxlength':5,'empty':True},'I1TRFC':{'type':'string','maxlength':20,'empty':True},'I1IGLC':{'type':'string','maxlength':10,'empty':True},'I1CRTQ':{'type':'string','maxlength':5,'empty':True},'I1VOL':{'type':'string','maxlength':9,'empty':True},'I1VLWT':{'type':'string','maxlength':9,'empty':True},'I1WGHN':{'type':'string','maxlength':9,'empty':True},'I1WGHT':{'type':'string','maxlength':9,'empty':True},'I1LNG':{'type':'string','maxlength':7,'empty':True},'I1WDTH':{'type':'string','maxlength':7,'empty':True},'I1DPTH':{'type':'string','maxlength':7,'empty':True},'I1EXTV':{'type':'string','maxlength':9,'empty':True},'I1EXTM':{'type':'string','maxlength':2,'empty':True},'I1IBCD':{'type':'string','maxlength':20,'empty':True},'I1IWSC':{'type':'string','maxlength':3,'empty':True},'I1REGD':{'type':'string','maxlength':45,'empty':True},'I1REGU':{'type':'string','maxlength':10,'empty':True},'I1CHGZ':{'type':'string','maxlength':45,'empty':True},'I1CHGU':{'type':'string','maxlength':10,'empty':True},'I1LANG':{'type':'string','maxlength':3,'empty':True},'I1EDTT':{'type':'string','maxlength':3,'empty':True},'I1TXC1':{'type':'string','maxlength':3,'empty':True},'I1TXP1':{'type':'string','maxlength':6,'empty':True},'I1TXC2':{'type':'string','maxlength':3,'empty':True},'I1TXP2':{'type':'string','maxlength':6,'empty':True},'I1TXC3':{'type':'string','maxlength':3,'empty':True},'I1TXP3':{'type':'string','maxlength':6,'empty':True},'I1TXC4':{'type':'string','maxlength':3,'empty':True},'I1TXP4':{'type':'string','maxlength':6,'empty':True},'I1TXC5':{'type':'string','maxlength':3,'empty':True},'I1TXP5':{'type':'string','maxlength':6,'empty':True}}
DLIM00P_record = DLIM00P_encoding.keys()
llmigration_table='item_master'
input_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIM00P/DLIM00P.tsv'
skip_record = False

# counters
line_count = 0
write_count = 0

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
        log_message['loggily error'] = response
        log_json_message(log_message)
    log_message={}
            
def database_insert(insert_record):
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, insert_record.values())
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_insert'] = str(error)
        log_json_message(log_messages)
        sys.exit()

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

# build output dict
#output_record = {}
#for x in range(0, len(DLIM00P_Field_format),3):
#    output_record[DLIM00P_Field_format[x]] = ''


with open(input_filename) as csv_file:
    item_master = csv.DictReader(csv_file, delimiter='\t')
    for row in item_master:
        line_count += 1
        # transform output record to field specifications
        log_messages = {}
        # create insert statement
        placeholders = ', '.join(['%s'] * len(row))
        
        output_record = {}
        for x in range(0, len(row)*3,3):
            output_record[DLIM00P_Field_format[x]] = ''
        
        columns = ', '.join(output_record.keys())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
            
        try:
            cursor.execute(sql, list(row.values()))
            connection.commit()
        except mysql.connector.Error as error:
            log_messages['MySQL_Insert'] = str(error)
            log_json_message(log_messages)
            print(error)
            
# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    print("MySQL connection is closed")            


log_messages['Records Processed']= line_count
log_messages['Records Written to database']= write_count
log_json_message(log_messages)
sys.exit()