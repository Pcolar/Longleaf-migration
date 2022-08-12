import json
import csv
import datetime, time
import os, sys
import requests
import regex, re
from cerberus import Validator
import mysql.connector

# hidden parameters
from secrets import *
# field mapper and formats
from DLIC00P_map  import *
from DLIC00P_format import *
#from DLIC00P_dataclass import *

# Globals
log_messages={}
DLIC00P_encoding = {'I5I':'ascii','I5ICT':'ascii','I5EFDT':'ascii','I5ICC':'ascii','I5ICE':'ascii'}
# {'C1CN': 'ascii','C1REGN': 'utf-8','C1PIDN': 'utf-8','C1PCNM': 'utf-8','C1PAD0': 'utf-8','C1PAD1': 'utf-8','C1PAD2': 'utf-8','C1PAD3': 'utf-8','C1PAD4': 'utf-8','C1PAD5': 'ascii','C1PAD6': 'utf-8','C1PDLVN': 'ascii','C1DXNO': 'ascii','C1DXLOC': 'ascii','C1PHN': 'ascii','C1FAX': 'ascii','C1CSTS': 'ascii','C1CONO': 'ascii','C1BR': 'ascii','C1RGN': 'ascii','C1SLRP': 'ascii','C1CDIS': 'ascii','C1PRCD': 'ascii','C1XINV': 'ascii', 'C1XNVF': 'ascii', 'C1CTXC': 'ascii', 'C1TXNO': 'ascii', 'C1PYTC': 'ascii', 'C1ILIC': 'ascii', 'C1SAN': 'ascii', 'C1BN': 'ascii', 'C1CCLS': 'ascii', 'C1EXCD': 'ascii', 'C1CRLM': 'ascii', 'C1CIAC': 'ascii', 'C1IBR': 'ascii', 'C1WH': 'ascii', 'C1CAR': 'ascii', 'C1FAGC': 'ascii', 'C1RUN': 'ascii', 'C1DSTP': 'ascii', 'C1STTY': 'ascii', 'C1CMNT': 'utf-8', 'C1OIF': 'ascii', 'C1DLVP': 'ascii', 'C1DLOP': 'ascii', 'C1CBOA': 'ascii', 'C1OCON': 'ascii', 'C1SBI': 'ascii', 'C1ONRF': 'ascii', 'C1CUTO': 'ascii', 'C1MIF': 'ascii', 'C1FSCF': 'ascii', 'C1CIEC': 'ascii', 'C1INCA': 'ascii', 'C1DFCF': 'ascii', 'C1AFCG': 'ascii', 'C1CFRT': 'ascii', 'C1CL1': 'ascii', 'C1CL2': 'ascii', 'C1CL3': 'ascii', 'C1CL4': 'ascii', 'C1CL5': 'ascii', 'C1MJS': 'ascii', 'C1STN': 'ascii', 'C1BOCR': 'ascii', 'C1ECR': 'ascii', 'C1STMT': 'ascii', 'C1RLSP': 'ascii', 'C1CTO': 'ascii', 'C1CASC': 'utf-8', 'C1REGZ': 'ascii', 'C1REGU': 'ascii', 'C1CHGZ': 'ascii', 'C1CHGU': 'ascii'}
DLIC00P_validator_schema = {'I5I':{'type':'string','empty':False,'maxlength':20},'I5ICT':{'type':'string','empty':False,'maxlength':8},'I5EFDT':{'type':'string','empty':False,'maxlength':10},'I5ICC':{'type':'string','empty':True,'maxlength':8},'I5ICE':{'type':'string','empty':True,'maxlength':8}}
DLIC00P_record = DLIC00P_encoding.keys()
llmigration_table='item_class'
input_filename = [
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-1.csv',
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-2.csv',
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-3.csv']
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-220810.tsv'
skip_record = False
item_dict = {}
class_aggregator = {}

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
            
def database_insert(insert_record):
    placeholders = ', '.join(['%s'] * len(insert_record))
    columns = ', '.join(insert_record.keys())
    global insert_count, skip_record
    # fix for utf-8 keys
    columns = columns.replace('\ufeff','')
    try:
        qry = "INSERT INTO %s ( %s ) VALUES ( %s )" % (llmigration_table, columns, placeholders)
        cursor.execute(qry, list(insert_record.values()))
        connection.commit()
        insert_count += 1
    except mysql.connector.DatabaseError as error:
        log_messages['MySQL_insert'] = str(error)
        log_messages['Record ID'] = insert_record['I5I']
        log_json_message(log_messages)
        skip_record = True
        
def DLIC00P_validate_fields(record):
    global skip_record
    # field specific mapping
    # normalize ISBN13
    if len(record['I5I']) < 8:
        record['I5I'] = record['I5I'].strip(' -.''')

    # validate fields
    for field_index in range(0, len(DLIC00P_Field_format),3):
        field_type = field_index + 1
        field_length = field_index + 2
        if record[DLIC00P_Field_format[field_index]]:
            log_messages['field'] = DLIC00P_Field_format[field_index]
            if  len(record[DLIC00P_Field_format[field_index]]) >  int(DLIC00P_Field_format[field_length]):
                log_messages['length is greater than'] = DLIC00P_Field_format[field_length]
                skip_record = True
            if DLIC00P_Field_format[field_type] == "A":
                if not alpha.match(record[DLIC00P_Field_format[field_index]]):
                    log_messages['field is not alpha'] = record[DLIC00P_Field_format[field_index]]
                    skip_record = True
            if DLIC00P_Field_format[field_type] == "N":
                if not alpha.match(record[DLIC00P_Field_format[field_index]]):
                    log_messages['field is not a number'] = record[DLIC00P_Field_format[field_index]]
                    skip_record = True
    if skip_record:
        log_json_message(log_messages)
    
def write_output(record):   
    global skip_record, skipped_count, write_count 
    # Set Default
    if not skip_record:
        DLIC00P_validate_fields(record)
    # output record    
    if not skip_record:
        # validate output record to specification
        if not v.validate(output_record):
            log_messages= v.errors
            log_messages['Record ID'] = record['I5I']
            log_messages['Status'] = 'record skipped'           
            log_json_message(log_messages)
            skipped_count += 1
            #loggily_json_message(log_messages)
        else:
            values = record.values()
            database_insert(record)
            if not skip_record:
                csvwriter.writerow(values)        
                write_count += 1
    else:
        skipped_count += 1
           
### MAIN ###  
# field validator setup
v = Validator(DLIC00P_validator_schema)
v.allow_unknown = True
# class_aggregator mapping to use the field name instead of an index

# connect to DB
try: 
    connection = mysql.connector.connect(
        host=llmigration_host,
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
    #print("You're connected to database: ", record) 

# open output file
output_file = open(output_filename, 'w')
csvwriter = csv.writer(output_file, delimiter='\t')

#log_messages['File created'] = output_filename
#log_json_message(log_messages)

# for each input record, fill in the data structure for a specific Product ID
# save the Product ID and data class in a Dict
for input_file_num in range(0,2):
    input_file = input_filename[input_file_num]
    with open(input_file) as csv_file:
        item_rec = csv.DictReader(csv_file, delimiter=',')
        for row in item_rec:
            #line_count += 1
            # load input data into dict/class
            skip_record = False
            log_messages = {}
            process_record = {}
            # initialize item_class keys
            for x in range(0, len(class_aggregator_map)):
                process_record[class_aggregator_map[x]] = ''
            
            for col in field_map.keys():
                # move data to output column if not null
                try:
                    if row[field_map[col]]:
                        process_record[col] = row[field_map[col]]
                except KeyError: pass
               
            # create/retrieve & update the dict entry
            try:
                # retrieve and update the data structure
                item_record = list(item_dict[process_record['Product_ID']])
                for x in range(0, len(class_aggregator_map)):
                    if (not item_record[x]) and process_record[class_aggregator_map[x]]:
                        item_record[x] = process_record[class_aggregator_map[x]]
            except KeyError:
                # dict entry does not exist, so create
                item_record = process_record.values()
            
            item_dict[process_record['Product_ID']] = item_record
    print('Processing complete for ', input_file)


# DLIC00P-3 drives the creation of multiple records
input_file = input_filename[2]
with open(input_file) as csv_file:
    item_rec = csv.DictReader(csv_file, delimiter=',')
    for row in item_rec:
        skip_record = False
        log_messages = {}
        process_record = {}
        output_record = {}
        # initialize item_class keys
        for x in range(0, len(class_aggregator_map)):
            process_record[class_aggregator_map[x]] = ''
        
        for col in field_map.keys():
            # move data to output column if not null
            try:
                if row[field_map[col]]:
                    process_record[col] = row[field_map[col]]
            except KeyError: pass
            
        # retrieve the dict entry and create records
        try:
            # retrieve and update the data structure
            item_record = list(item_dict[process_record['Product_ID']])
            for x in range(0, len(class_aggregator_map)):
                if (not item_record[x]) and process_record[class_aggregator_map[x]]:
                    item_record[x] = process_record[class_aggregator_map[x]]
        except KeyError:
            # dict entry does not exist
            log_messages['Matching record not found'] = process_record['Product_ID']
            log_messages['ISBN13'] = process_record['ISBN13']
            log_json_message(log_messages)
            skip_record = True
            
        # Field specific mappings
        # move content to output record
        if process_record['ISBN13']:
            output_record['I5I'] = process_record['ISBN13'].zfill(8)
        else:
            # no ISBN
            skip_record = True
        # set uniform effective date
        output_record['I5EFDT'] = '2022-08-01'
        output_record['I5ICE'] = ''
        # mappings
        # IC-RGHTI/E
        if item_record[Rights_Code] == 'A':
            output_record['I5ICT'] = 'IC-RGHTI'
            output_record['I5ICC'] = '***'
            write_output(output_record)
        else:
            if item_record[Rights_Code] == 'B':
                output_record['I5ICT'] = 'IC-RGHTE'
                output_record['I5ICC'] = item_record[Cannot_Distribute_Countries_Country_ID]
                write_output(output_record)
            else:
                if item_record[Rights_Code] == 'C':
                    output_record['I5ICT'] = 'IC-RGHTI'
                    output_record['I5ICC'] = item_record[Can_Distribute_Countries_Country_ID] 
                    write_output(output_record)
                        
# process all entries in the aggregator DICT
for item_record in item_dict.items():
    # move content to output record
    output_record['I5I'] = item_record[ISBN13]
    # set uniform effective date
    output_record['I5EFDT'] = '2022-08-01'
    output_record['I5ICE'] = ''
    # mappings
    # IC-SER
    try:
        output_record['I5ICT'] = 'IC-SER'
        output_record['I5ICC'] = ic_ser_map[item_record[Series_ID]]
        write_output(output_record)
    except KeyError:
        log_messages['IC-SER map not found'] = item_record[Series_ID]
    # IC-SUBM
    try:
        output_record['I5ICT'] = 'IC-SUBM'
        output_record['I5ICC'] = ic_subm_map[item_record[Minor_Disc_ID]]
        write_output(output_record)
    except KeyError:
        log_messages['IC-SUBM map not found'] = item_record[Minor_Disc_ID]
    # IC-INT
    try:
        output_record['I5ICT'] = 'IC-INT'
        output_record['I5ICC'] = ic_int_map[item_record[Interest_Code]]
        write_output(output_record)
    except KeyError:
        log_messages['IC-INT map not found'] = item_record[Interest_Code]
    # IC-SEAS  
    output_record['I5ICT'] = 'IC-SEAS'
    output_record['I5ICC'] = item_record[Season]
    write_output(output_record)
    # IC-SUBJ
    output_record['I5ICT'] = 'IC-SUBJ'
    output_record['I5ICC'] = item_record[BISAC1][:-1]
    write_output(output_record)
    output_record['I5ICT'] = 'IC-SUBJ'
    output_record['I5ICC'] = item_record[BISAC2][:-1]
    write_output(output_record)
    output_record['I5ICT'] = 'IC-SUBJ'
    output_record['I5ICC'] = item_record[BISAC3][:-1]
    write_output(output_record)
    # IC-ROY
    # retrieve the corresponding item master
    try:
        qry = 'Select I1CLS, I1BRNC from item_master where I1I = %s'
        cursor.execute(qry, output_record['I5I'])
        connection.commit()
        item_master_rec = cursor.fetchall()
        if item_master_rec[0] == '013' or (item_master_rec[0] == '009' and item_master_rec[1] == 'DI'):
            output_record['I5ICC'] = 'N'
        else:
            output_record['I5ICC'] = ''
    except mysql.connector.DatabaseError as error:
        output_record['I5ICC'] = ''
        log_messages['MySQL_query'] = str(error)
        log_messages['IC-ROY item_master not found'] = output_record['I5I']    
    output_record['I5ICT'] = 'IC-ROY'
    write_output(output_record)
        
        
# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Processed']= line_count
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()