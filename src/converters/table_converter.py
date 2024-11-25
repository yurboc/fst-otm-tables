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
        # Select 'sheets' module with API v4
        self.service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)

    def setSpreadsheetId(self, spreadsheetId):
        self.spreadsheetId = spreadsheetId

    def setSpreadsheetRange(self, spreadsheetRange):
        self.spreadsheetRange = spreadsheetRange
    
    def getContents(self):
        return self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheetId, range=self.spreadsheetRange).execute()

    def saveToFile(self, filename):
        with open(filename, 'w') as f:
            json.dump(self.getContents(), f, indent=4, ensure_ascii=False)
