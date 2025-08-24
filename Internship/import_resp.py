
import os
import xml.etree.ElementTree as ET
import pandas as pd
import uuid
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, update, Table, Column, Integer, String, MetaData, inspect, text, cast
import numpy as np
import shutil
from sqlalchemy.dialects.postgresql import UUID
import logging

SGN_EXTENSION = ".sgn"
XML_EXTENSION = ".xml"
NAMESPACE_URI = '{http://ebxml.customs.go.th/XMLSchema/CustomsResponse_8_00}'

db_config = {

    'dbname' : '67C_Monitoring',
    'user' : 'postgres',
    'password' : 'MyTTT%401234',
    'host' : '150.95.25.8',
    'port' : 5432
}

db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(db_url)

db_config1 = {
    'dbname': 'import',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

db_url1 = f"postgresql://{db_config1['user']}:{db_config1['password']}@{db_config1['host']}:{db_config1['port']}/{db_config1['dbname']}"
engine1 = create_engine(db_url1)

def generate_unique_id():
    return str(uuid.uuid4())

def process_element_xml(element, namespaces, parent_prefix=""):
    data = {}
    for child in element:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        full_tag = f"{parent_prefix}_{tag}" if parent_prefix else tag
        text = child.text.strip() if child.text else None
        if text:
            data[full_tag] = text
        child_data = process_element_xml(child, namespaces, parent_prefix=full_tag)
        data.update(child_data)
    return data
    
def get_namespaces(xml_file):
    namespaces = {}
    events = "start", "start-ns"
    for event, elem in ET.iterparse(xml_file, events):
        if event == 'start-ns':
            namespaces[elem[0]] = elem[1]
        elif event == 'start':
            break
    return namespaces

def process_file(full_path):
    detail_response_list = []
    doc_response_list = []

    try:
        namespaces = get_namespaces(full_path)
        with open(full_path, 'r', encoding='utf-8') as file:  
            tree = ET.parse(file)
        root = tree.getroot()
        
        for doc_response_element in root.findall('.//DocumentControl', namespaces):
            doc_response_data = process_element_xml(doc_response_element, namespaces)
            doc_response_data['ird_filename'] = os.path.basename(full_path)
            date_time_str = os.path.basename(full_path).split("_")[6]
            doc_response_data['ird_created_date'] = datetime.strptime(date_time_str, "%Y%m%d%H%M%S")
            doc_response_data['ird_id'] = generate_unique_id()
            doc_response_list.append(doc_response_data)

        for detail_response_element in root.findall('.//DocumentDetail', namespaces):
            detail_response_data = process_element_xml(detail_response_element, namespaces)
            detail_response_data['irt_id'] = generate_unique_id()
            detail_response_data['irt_ird_id'] = doc_response_data['ird_id']
            detail_response_list.append(detail_response_data)
    
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"Error processing file {full_path}: {e}")

    return doc_response_list, detail_response_list

all_doc_response_data = []
all_good_response_data = []
file_count = 0
failed_files = []

pathFolder = os.environ['PATH_FOLDER']
error_directory = os.environ['ERROR_DIRECTORY'] 

os.makedirs(error_directory, exist_ok=True)

for root, dirs, files in os.walk(pathFolder):
    for filename in files:
        if filename.endswith(XML_EXTENSION):
            full_path = os.path.join(root, filename)
            try:
                doc_response_data, detail_response_data = process_file(full_path)
                if doc_response_data and detail_response_data:  
                    all_doc_response_data.extend(doc_response_data)
                    all_good_response_data.extend(detail_response_data)
                    file_count += 1  
                    print(f"{filename} processed successfully.")

                else:
                    print(f"{filename}, processed but no data found.")
                    failed_files.append(filename)
                    shutil.move(full_path, os.path.join(error_directory, filename))
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
                failed_files.append(filename)
                shutil.move(full_path, os.path.join(error_directory, filename))

print(f"Total files processed successfully: {file_count}")
if failed_files:
    print("Failed to process the following files:")
    for file in failed_files:
        print(file)


#สร้าง DataFrame (df_doc_response กับ df_detail_response)
df_doc_response = pd.DataFrame(all_doc_response_data)
df_detail_response = pd.DataFrame(all_good_response_data)

#filter MessageType 
df_doc_response = df_doc_response.loc[df_doc_response['MessageType'].isin(['IDCR', 'IDCA', 'PMTR', 'PMTA'])]

#filter df_detail_response จาก ird_id ของ df_doc_response
doc_ids = set(df_doc_response['ird_id'])
df_detail_response = df_detail_response[df_detail_response['irt_ird_id'].isin(doc_ids)]


message_columns = [col for col in df_doc_response.columns if '_Message' in col]
df_doc_response['ird_message'] = df_doc_response[message_columns].apply(lambda x: ' | '.join(x.dropna()), axis=1)
df_doc_response.drop(columns=message_columns, inplace=True)

df_doc_response['DocumentDetail_DeclarationAccept_DeclarationNumber'].fillna(df_doc_response['DocumentDetail_DeclarationPayment_DeclarationNumber'], inplace=True)
df_doc_response = df_doc_response.drop(columns=['DocumentDetail_DeclarationPayment_DeclarationNumber'])

columns_to_keep = ['DocumentDetail_DeclarationAccept_DeclarationNumber']
columns_to_drop = df_doc_response.filter(regex='^DocumentDetail_').columns.difference(columns_to_keep)
df_doc_response = df_doc_response.drop(columns=columns_to_drop, axis=1)


message_columns = [col for col in df_detail_response.columns if '_Message' in col]
df_detail_response['irt_message'] = df_detail_response[message_columns].apply(lambda x: ' | '.join(x.dropna()), axis=1)
df_detail_response.drop(columns=message_columns, inplace=True)

errorcode_columns = [col for col in df_detail_response.columns if '_ErrorCode' in col]
df_detail_response['irt_errorcode'] = df_detail_response[errorcode_columns].apply(lambda x: ' | '.join(x.dropna()), axis=1)
df_detail_response.drop(columns=errorcode_columns, inplace=True)

df_detail_response['DeclarationAccept_DeclarationNumber'].fillna(df_detail_response['DeclarationPayment_DeclarationNumber'], inplace=True)
df_detail_response = df_detail_response.drop(columns=['DeclarationPayment_DeclarationNumber'])

from import_mapping import mapping_document_resp, mapping_detail_resp

df_doc_response.rename(columns=mapping_document_resp, inplace=True)
df_detail_response.rename(columns=mapping_detail_resp, inplace=True)


query = """
SELECT ic_id, ic_reference_number 
FROM import_control_up
"""
#engine(.8)
df_document = pd.read_sql(query, engine)


#หา FK ของ ic_id (df_doc_response)
df_map = df_document.set_index('ic_reference_number')['ic_id'].to_dict()
df_doc_response['ird_ic_id'] = df_doc_response['ird_reference_number'].map(df_map)


# ---------------------------------------------------------------------------------------------------------------------------

df_doc_response.to_sql('doc_resp2', engine1, if_exists='append', index=False) #append insert
df_detail_response.to_sql('detail_resp2', engine1, if_exists='append', index=False) #replace ลบและสร้างใหม่

# ส่วนที่ update ลง control ----------------------------------------------------------------------------------------------------

query = """
SELECT rs_id, rs_type, rs_message, rs_status_id, rs_status_priority
FROM response_status
"""
df_response_status = pd.read_sql(query, engine)

#df_doc_response กับ status (**ird_id มีค่าซ้ำกันตอน merge กับ status)
merged_status = pd.merge(df_doc_response, df_response_status, 
                     left_on=['ird_message_type', 'ird_message'], 
                     right_on=['rs_type', 'rs_message'],
                     how='left')
merged_status.head(3)

#selected_rows คือ rs_status_priority(max) จากนั้น groupby ตาม ird_ic_id
merged_status['rs_status_priority'] = pd.to_numeric(merged_status['rs_status_priority'], errors='coerce')

max_priority = merged_status.groupby('ird_ic_id')['rs_status_priority'].transform('max')

max_priority_filter = merged_status['rs_status_priority'] == max_priority

selected_rows = merged_status[max_priority_filter]
selected_rows.head(3)

# ใช้ groupby และ first เพื่อเลือกค่าแรกที่เจอ เพราะบางตัว rs_status_priority เท่ากัน 
ic_rs_id_map = selected_rows.groupby('ird_ic_id')['rs_id'].first()
ic_rs_map_status = selected_rows.groupby('ird_ic_id')['rs_status_priority'].first()
ic_declaration_no_map = selected_rows.groupby('ird_ic_id')['ird_declaration_number'].first()

df_document['ic_rs_id'] = df_document['ic_id'].map(ic_rs_id_map)
df_document['ic_rs_status_priority'] = df_document['ic_id'].map(ic_rs_map_status)
df_document['ic_declaration_no'] = df_document['ic_id'].map(ic_declaration_no_map)

df_document_update = df_document
df_document_update


import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, update, cast
from sqlalchemy.dialects.postgresql import UUID
import os

db_config1 = {
    'dbname': 'import',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

db_url1 = f"postgresql://{db_config1['user']}:{db_config1['password']}@{db_config1['host']}:{db_config1['port']}/{db_config1['dbname']}"
engine1 = create_engine(db_url1)

from sqlalchemy import create_engine, MetaData, Table, select, update, cast

def update_import_control_up(engine1, df_document_update):
    metadata = MetaData()
    control = Table('import_control_up', metadata, autoload_with=engine1)

    with engine1.connect() as conn:
        with conn.begin():  # Use transaction
            for _, row in df_document_update.iterrows():
                if pd.notna(row['ic_rs_id']) and pd.notna(row['ic_declaration_no']) and pd.notna(row['ic_rs_status_priority']):
                    ic_id = str(row['ic_id'])  # Ensure ic_id is treated as a string

                    # Fetch the current ic_rs_status_priority from the database
                    select_stmt = select(control.c.ic_rs_status_priority).where(cast(control.c.ic_id, UUID) == ic_id)
                    result = conn.execute(select_stmt).fetchone()

                    if result:
                        current_priority = result[0]
                        new_priority = row['ic_rs_status_priority']

                        # Check if the current priority is null or if the new priority is greater than the current priority
                        if current_priority is None or new_priority > current_priority:
                            stmt = (
                                update(control)
                                .where(cast(control.c.ic_id, UUID) == ic_id)
                                .values(
                                    ic_rs_id=row['ic_rs_id'],
                                    ic_declaration_no=row['ic_declaration_no'],
                                    ic_rs_status_priority=new_priority
                                )
                            )
                            conn.execute(stmt)

update_import_control_up(engine1, df_document_update)

