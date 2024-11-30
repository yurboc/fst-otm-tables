from modules import table_converter
from modules import table_uploader
import json
import os

CRED_GOOGLE = 'credentials/cred-google.json'
CRED_FTP = 'credentials/cred-ftp.json'
OUT_DIR = 'output'
CONFIG_FILE = 'config.json'

# Load config from JSON
def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

# Get output file path
def get_output_file_path(file_name):
    return os.path.join(OUT_DIR, file_name)

# Convert one table from Google Spreadsheet to JavaScript (JSON)
def convert_table(converter, table_params):
    print("Converting table: ", table_params['generator_name'])
    output_file = get_output_file_path(table_params['output_file'])
    converter.setSpreadsheetId(table_params['spreadsheetId'])
    converter.setSpreadsheetRange(table_params['range'])
    converter.readTable()
    converter.parseData(table_params['fields'])
    converter.saveToFile(output_file)
    print("Done table: ", table_params['generator_name'])

# Upload generated JavaScript file to FTP
def upload_table(uploader, table_params):
    print("Uploading file: ", table_params['output_file'])
    uploader.upload(table_params, local_dir=OUT_DIR)
    print("Done file: ", table_params['output_file'])

# MAIN
if __name__ == '__main__':
    print('Start converting...')
    converter = table_converter.TableConverter(CRED_GOOGLE)
    uploader = table_uploader.TableUploader(CRED_FTP)
    converter.auth()
    uploader.start()
    config = load_config(CONFIG_FILE)
    for table_params in config:
        convert_table(converter, table_params)
        upload_table(uploader, table_params)
    uploader.quit()
    print('Done converting!')