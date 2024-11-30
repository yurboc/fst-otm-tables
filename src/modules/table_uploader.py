import json
import ftplib
import os

class TableUploader:
    def __init__(self, credentials):
        self.credentials = credentials

    def start(self):
        # Read credentials from file
        self.cred = json.load(open(self.credentials))
        self.session = ftplib.FTP(self.cred['server'], self.cred['username'], self.cred['password'])

    def upload(self, table_params, local_dir):
        file_name = table_params['output_file']
        src_file = os.path.join(local_dir, file_name)
        dst_file = os.path.basename(table_params['remote_file'])
        dst_path = os.path.dirname(table_params['remote_file'])
        self.session.cwd(dst_path)
        self.session.storbinary('STOR ' + dst_file, open(src_file, 'rb'))
        print("Uploaded file: ", file_name)

    def quit(self):
        self.session.quit()
