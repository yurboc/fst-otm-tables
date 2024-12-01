from dateutil import parser
import datetime
import json
import httplib2 
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

class TableConverter:
    def __init__(self, credentials):
        self.credentials = credentials

    def auth(self):
        # Read credentials from file
        cred = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        # Authorize into system
        httpAuth = cred.authorize(httplib2.Http()) 
        # Select service modules
        self.service_sheets = apiclient.discovery.build('sheets', 'v4', http = httpAuth)
        self.service_drive = apiclient.discovery.build('drive', 'v3', http = httpAuth)

    def setSpreadsheetId(self, spreadsheetId):
        self.spreadsheetId = spreadsheetId

    def setSpreadsheetRange(self, spreadsheetRange):
        self.spreadsheetRange = spreadsheetRange
    
    def readTable(self):
        self.rawData = self.service_sheets.spreadsheets().values().get(spreadsheetId=self.spreadsheetId, range=self.spreadsheetRange).execute()

    def parseData(self, fields):
        self.combinedData = list()
        # Iterate over rows
        for rowData in self.rawData['values']:
            tmpRow: dict[str, str] = dict()
            # Iterate over columns
            for field in fields:
                col_id = field["col"]
                col_name = field["name"]
                tmpRow[col_name] = rowData[col_id] if col_id < len(rowData) else ""
            # Save row
            self.combinedData.append(tmpRow)
        # Get date of last update and generation
        self.docName = self.service_drive.files().get(fileId=self.spreadsheetId, fields="name, modifiedTime").execute().get("name")
        modifiedTime = self.service_drive.files().get(fileId=self.spreadsheetId, fields="name, modifiedTime").execute().get("modifiedTime")
        modifiedTimeParsed = parser.parse(modifiedTime)
        self.lastUpdateDate = modifiedTimeParsed.strftime("%d.%m.%Y %H:%M:%S")
        self.generationDate = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        # Print details
        print(f"File name:       {self.docName}")
        print(f"Last update:     {self.lastUpdateDate}")
        print(f"Generation date: {self.generationDate}")

    def saveToFile(self, filename):
        with open(filename, 'w') as f:
            f.write("var php_data = ")
            f.write(json.dumps(self.combinedData, separators=(',', ':'), ensure_ascii=False))
            f.write(";\n")
            f.write(f"var modified_date=\"{self.lastUpdateDate}\";\n")
            f.write(f"var generated_date=\"{self.generationDate}\";\n")
        print(f"Saved to file:   {filename}")