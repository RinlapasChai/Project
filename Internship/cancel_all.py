
import os
import xml.etree.ElementTree as ET
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine
import shutil

SGN_EXTENSION = ".sgn"
XML_EXTENSION = ".xml"

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
    doc_cancel_list = []

    try:
        namespaces = get_namespaces(full_path)
        with open(full_path, 'r', encoding='utf-8') as file:  
            tree = ET.parse(file)
        root = tree.getroot()
        
        for doc_cancel_element in root.findall('.//DocumentControl', namespaces):
            doc_cancel_data = process_element_xml(doc_cancel_element, namespaces)
            doc_cancel_data['cc_filename'] = os.path.basename(full_path)
            date_time_str = os.path.basename(full_path).split("_")[3].split(".")[0]
            doc_cancel_data['cc_created_date'] = datetime.strptime(date_time_str, "%Y%m%d%H%M%S")
            doc_cancel_data['cc_id'] = generate_unique_id()
            doc_cancel_list.append(doc_cancel_data)

    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"Error processing file {full_path}: {e}")

    return doc_cancel_list


all_doc_cancel_data = []
success_count = 0
failed_files = []

pathFolder = os.environ['PATH_FOLDER_SGN']
error_directory = os.environ['ERROR_DIRECTORY_SGN'] 

for root, dirs, files in os.walk(pathFolder):
    for filename in files:
        if filename.startswith('ebxml_CANCEL') and filename.endswith(SGN_EXTENSION):  
            full_path = os.path.join(root, filename)
            try:
                doc_cancel_data = process_file(full_path)
                if doc_cancel_data:
                    all_doc_cancel_data.extend(doc_cancel_data)
                    success_count += 1
                    print(f"{filename} processed successfully.")
                else:
                    raise ValueError("No data found after processing.")
            except Exception as e:
                print(f"Failed to process {filename}: {e}")
                failed_files.append(filename)
                shutil.move(full_path, os.path.join(error_directory, filename))

print(f"Total files processed successfully: {success_count}")
if failed_files:
    print("Failed to process the following files:")
    for file in failed_files:
        print(file)


df_doc_cancel = pd.DataFrame(all_doc_cancel_data)
df_doc_cancel.head()

from import_mapping import mapping_cancel

df_doc_cancel.rename(columns=mapping_cancel, inplace=True)
df_doc_cancel.head()

db_config1 = {
    'dbname': 'import',
    'user': 'postgres',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

db_url1 = f"postgresql://{db_config1['user']}:{db_config1['password']}@{db_config1['host']}:{db_config1['port']}/{db_config1['dbname']}"
engine1 = create_engine(db_url1)

df_doc_cancel.to_sql('cancel', engine1, if_exists='append', index=False) #replace ลบและสร้างใหม่




