### DLIC00P - Item Classification File

import json
import csv
import datetime
import os, sys
import requests
import regex, re
from cerberus import Validator
import mysql.connector

# hidden parameters
from llsecrets import *
# field mapper and formats
from DLIC00P_map  import *
from DLIC00P_format import *
#from DLIC00P_dataclass import *

# Globals
log_messages={}
DLIC00P_encoding = {'I5I':'ascii','I5ICT':'ascii','I5EFDT':'ascii','I5ICC':'ascii','I5ICE':'ascii'}
DLIC00P_validator_schema = {'I5I':{'type':'string','empty':False,'maxlength':20},'I5ICT':{'type':'string','empty':False,'maxlength':8},'I5EFDT':{'type':'string','empty':False,'maxlength':10},'I5ICC':{'type':'string','empty':True,'maxlength':8},'I5ICE':{'type':'string','empty':True,'maxlength':8}}
DLIC00P_record = DLIC00P_encoding.keys()
llmigration_table='item_class'
input_filename = [
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-1.csv',
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-2.csv',
    '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-3.csv']
output_filename = '/Volumes/GoogleDrive/My Drive/UNC Press-Longleaf/DataSets/DLIC00P/DLIC00P-' + datetime.datetime.today().strftime('%Y%m%d') + '.tsv'
skip_record = False
item_dict = {}
class_aggregator = {}

# regex
alpha = regex.compile('\w*')
numb = regex.compile('\d*')
redact_char = re.compile('[\r\n\t]*')
redact_punct = re.compile(' -.')
# counters
line_count = 0
write_count = 0
insert_count = 0
skipped_count = 0

def log_json_message(log_message):
    """print out  in json tagged log message format"""
    #log_message['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    #log_message['program'] = os.path.basename(__file__)
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
        if not 'Duplicate' in str(error):
            log_messages = {}
            log_messages['MySQL_insert'] = str(error)
            log_messages['Record ID'] = insert_record['I5I']
            log_json_message(log_messages)
        skip_record = True
        
def DLIC00P_validate_fields(record):
    global skip_record
    # field specific mapping
    # normalize ISBN13
    record['I5I'] = record['I5I'].strip(' -.''')
    record['I5I'] = record['I5I'].zfill(8)
    
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
    if not record['I5I']:
        skip_record = True
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
    cursor = connection.cursor(buffered=True)
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
            
            # for file 1 - exclude Rutgers (02) and Truman State (12)
            if input_file_num == 0:
                if int(row['Company No']) == 2 or int(row['Company No']) == 12:
                    skip_record = True
               
            # create/retrieve & update the dict entry
            if not skip_record:
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
        output_record['I5I'] = process_record['ISBN13'].zfill(8)
        if not process_record['ISBN13']:
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
print('Processing complete for ', input_file)
                        
# process all entries in the aggregator DICT
for item_record in item_dict.items():
    skip_record = False
    # clear messages
    log_messages = {}
    # move content to output record
    output_record['I5I'] = item_record[1][ISBN13]
    log_messages['ISBN13'] = item_record[1][ISBN13]
    if not output_record['I5I']:
        skip_record = True
    else:
        output_record['I5I'] = re.sub('-','', output_record['I5I'])
        #output_record['I5I'] = re.sub('\.','', output_record['I5I'])
    # set uniform effective date
    output_record['I5EFDT'] = '2022-08-01'
    output_record['I5ICE'] = ''
    
    # retrieve the corresponding item record
    try:
        qry = 'Select Royalties_Flag, User_Field_Value_3 from informer_DLIM00P where I1I = %s or I1AI = %s'
        item_list = [output_record['I5I'], output_record['I5I']]
        cursor.execute(qry, item_list)
        connection.commit()
        item_master_rec = cursor.fetchone()
        if not item_master_rec:
            skip_record = True
            log_messages['item_master not found'] = output_record['I5I'] 
            output_record['I5ICC'] = 'N'
        else:
            Royalties_Flag = item_master_rec[0]
            User_Field_Value_3 = item_master_rec[1]
    except mysql.connector.DatabaseError as error:
        skip_record = True
        log_messages['MySQL_query'] = str(error)
        log_messages['item_master not found'] = output_record['I5I']
        skipped_count += 1
    if len(log_messages.keys()) > 1:
            log_json_message(log_messages)
            
    if not skip_record:
        # IC-ROY
        output_record['I5ICT'] = 'IC-ROY'
        if Royalties_Flag == 'Y':
            output_record['I5ICC'] = ''
        else:
            output_record['I5ICC'] = 'N'
        write_output(output_record)

        # IC-SER
        try:
            output_record['I5ICT'] = 'IC-SER'
            output_record['I5ICC'] = ic_ser_map[item_record[1][Series_ID]]
            write_output(output_record)
        except KeyError:
            log_messages['IC-SER map not found'] = item_record[1][Series_ID]
        # IC-SUBM
        try:
            output_record['I5ICT'] = 'IC-SUBM'
            output_record['I5ICC'] = ic_subm_map[item_record[1][Minor_Disc_ID]]
            write_output(output_record)
        except KeyError:
            log_messages['IC-SUBM map not found'] = item_record[1][Minor_Disc_ID]
        # IC-INT
        try:
            output_record['I5ICT'] = 'IC-INT'
            output_record['I5ICC'] = ic_int_map[item_record[1][Interest_Code]]
            write_output(output_record)
        except KeyError:
            log_messages['IC-INT map not found'] = item_record[1][Interest_Code]
        # IC-SEAS  
        output_record['I5ICT'] = 'IC-SEAS'
        output_record['I5ICC'] = item_record[1][Season]
        write_output(output_record)
        # IC-SUBJ
        output_record['I5ICT'] = 'IC-SUBJ'
        output_record['I5ICC'] = item_record[1][BISAC1][:-1]
        write_output(output_record)
        output_record['I5ICT'] = 'IC-SUBJ'
        output_record['I5ICC'] = item_record[1][BISAC2][:-1]
        write_output(output_record)
        output_record['I5ICT'] = 'IC-SUBJ'
        output_record['I5ICC'] = item_record[1][BISAC3][:-1]
        write_output(output_record)
        # IC-PROPR
        output_record['I5ICT'] = 'IC-PROPR'
        if User_Field_Value_3 == 'M' or User_Field_Value_3 == 'N':
            output_record['I5ICC'] = User_Field_Value_3
            write_output(output_record)      
        
# close database connection
if connection.is_connected():
    cursor.close()
    connection.close()
    # print("MySQL connection is closed")            

log_messages = {}
log_messages['Records Written to output file']= write_count
log_messages['Records Written to database']= insert_count
log_messages['Records Skipped'] = skipped_count
log_json_message(log_messages)
sys.exit()