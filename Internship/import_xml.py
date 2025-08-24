import os
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from sqlalchemy import create_engine
from import_mapping import column_mapping, mapping_invoice, mapping_detail, mapping_duty, mapping_permit

# dbname = '67C_Tiffa'
# user = 'postgres'
# password = 'postgres'
# host = 'localhost'
# port = 5432

db_config = {
    'dbname': 'import',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(db_url)

pathFolder_sgn = os.environ['PATH_FOLDER_SGN'] 

SGN_EXTENSION = ".sgn"
XML_EXTENSION = ".xml"
NAMESPACE_URI = '{http://ebxml.customs.go.th/XMLSchema/CustomsImportDeclaration_4_00}'

def generate_unique_id():
    return str(uuid.uuid4())

def process_element_xml(element, parent_prefix=""):
    data = {}
    for child in element:
        tag = child.tag.split('}')[-1]
        full_tag = f"{parent_prefix}_{tag}" if parent_prefix else tag
        text = child.text.strip() if child.text else None
        if text:
            data[full_tag] = text
        data.update(process_element_xml(child, full_tag))
    return data

def process_file(full_path):
    invoice_list = []
    detail_list = []
    duty_list = []
    document_list = []
    permit_list = []

    with open(full_path, 'rb') as file:
        tree = ET.parse(file)
    root = tree.getroot()
    
    for document_element in root.findall(f'{NAMESPACE_URI}DocumentControl'):
        document_data = process_element_xml(document_element)
        
        document_data['ic_id'] = generate_unique_id()
        document_data['ic_filename'] = os.path.basename(full_path)
        date_time_str = os.path.basename(full_path).split("_")[3]
        document_data['ic_created_date'] = datetime.strptime(date_time_str, "%Y%m%d%H%M%S")
        document_list.append(document_data)

    for invoice_element in root.findall(f'.//{NAMESPACE_URI}GoodsShipment/{NAMESPACE_URI}Invoice'):
        invoice_data = process_element_xml(invoice_element)
        invoice_data['ii_id'] = generate_unique_id()
        invoice_data['ii_ic_id'] = document_data['ic_id']
        invoice_list.append(invoice_data)

    for detail_element in root.findall(f'.//{NAMESPACE_URI}Detail'):
        
        detail_data = process_element_xml(detail_element)
        detail_data['ide_id'] = generate_unique_id()
        detail_data['ide_ii_id'] = invoice_data['ii_id']
        
        detail_list.append(detail_data)
        
        for duty_element in detail_element.findall(f'{NAMESPACE_URI}Duty'):
            duty_data = process_element_xml(duty_element)
           
            duty_data['idu_id'] = generate_unique_id()
            duty_data['idu_ide_id'] = detail_data['ide_id']  # Foreign Key
            duty_list.append(duty_data)
        
        for permit_element in detail_element.findall(f'{NAMESPACE_URI}Permit'):
            permit_data = process_element_xml(permit_element)
           
            permit_data['ip_id'] = generate_unique_id()
            permit_data['ip_ide_id'] = detail_data['ide_id']  # Foreign Key
            permit_list.append(permit_data)
    return document_list, invoice_list,detail_list, duty_list ,permit_list

all_detail_data = []
all_duty_data = []
all_document_data = []
all_invoice_data = []
all_permit_data = []
# print(pathFolder_sgn)
for root, dirs, files in os.walk(pathFolder_sgn):
    for filename in files:
        if filename.startswith('ebxml_IMDECL') and filename.endswith(SGN_EXTENSION):
            full_path = os.path.join(root, filename)
            document_data,invoice_data,detail_data, duty_data ,permit_data = process_file(full_path)
            all_document_data.extend(document_data)
            all_invoice_data.extend(invoice_data)
            all_detail_data.extend(detail_data)
            all_duty_data.extend(duty_data)
            all_permit_data.extend(permit_data)

# Convert data to DataFrames
df_document = pd.DataFrame(all_document_data)
df_invoice = pd.DataFrame(all_invoice_data)
df_detail = pd.DataFrame(all_detail_data)
df_duty = pd.DataFrame(all_duty_data)
df_permit = pd.DataFrame(all_permit_data)


df_document.rename(columns=column_mapping, inplace=True)
df_document['ic_profile'] = df_document['ic_referencenumber'].str.slice(0, 4)
for new_col in ['ic_rs_id', 'ic_declaration_no', 'ic_rs_status_priority']:
    df_document.loc[:, new_col] = np.nan
df_document.to_sql('control', con=engine, if_exists='append', index=False) 


df_invoice.rename(columns=mapping_invoice, inplace=True)
df_invoice.to_sql('import_invoice_up', con=engine, if_exists='append', index=False)


columns_to_drop = [col for col in df_detail.columns if col.startswith('Duty') or col.startswith('Permit')]
df_detail = df_detail.drop(columns=columns_to_drop)
df_detail.rename(columns=mapping_detail, inplace=True)
df_detail.to_sql('import_detail_up', con=engine, if_exists='append', index=False)


columns_drop_duty = [col for col in df_duty.columns if col.startswith('Deposit')]
df_duty = df_duty.drop(columns=columns_drop_duty)
df_duty.rename(columns=mapping_duty, inplace=True)
df_duty.to_sql('import_duty_up', con=engine, if_exists='append', index=False)


df_permit.rename(columns=mapping_permit, inplace=True)
df_permit.to_sql('import_permit_up', con=engine, if_exists='append', index=False)
