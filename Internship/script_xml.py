
import subprocess
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor, CellExecutionError
import os

script_directory = r'C:\Users\rinlapas\Desktop\script_read_XML'
import_mapping = os.path.join(script_directory, 'import_mapping.py')
import_resp = os.path.join(script_directory, 'import_resp.py')

# Paths for import_resp
pathFolder = r'C:\Users\rinlapas\Desktop\Import Declaration\success_directory1-5\20240401'
error_directory = r'C:\Users\rinlapas\Desktop\Palm\Import Declaration\error_directory'

os.environ['PATH_FOLDER'] = pathFolder
os.environ['ERROR_DIRECTORY'] = error_directory

# Function to run a Python script
def run_python_script(script_path):
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    print(f"Script {script_path} executed successfully with output:\n{result.stdout}")
    if result.stderr:
        print(f"Errors:\n{result.stderr}")

run_python_script(import_mapping)
run_python_script(import_resp)

print("Scripts executed successfully")