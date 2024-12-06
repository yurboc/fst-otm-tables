from oauth2client.service_account import ServiceAccountCredentials
from dateutil import parser
import datetime
import json
import httplib2
import apiclient.discovery
import logging

logger = logging.getLogger(__name__)


class TableConverter:
    def __init__(self, credentials):
        self.credentials = credentials

    def auth(self):
        # Read credentials from file
        logger.info(f"Authenticating to Google...")
        cred = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials,
            [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        # Authorize into system
        httpAuth = cred.authorize(httplib2.Http())
        # Select service modules
        self.service_sheets = apiclient.discovery.build("sheets", "v4", http=httpAuth)
        self.service_drive = apiclient.discovery.build("drive", "v3", http=httpAuth)
        logger.info("Done authenticating to Google")

    def setSpreadsheetId(self, spreadsheetId):
        self.spreadsheetId = spreadsheetId

    def setSpreadsheetRange(self, spreadsheetRange):
        self.spreadsheetRange = spreadsheetRange

    def readTable(self):
        logger.info(f"Reading table {self.spreadsheetId}...")
        self.rawData = (
            self.service_sheets.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheetId, range=self.spreadsheetRange)
            .execute()
        )
        logger.info(f"Done reading table {self.spreadsheetId}")

    def parseData(self, fields):
        logger.info("Parsing data...")
        self.combinedData = list()
        # Iterate over rows
        for rowData in self.rawData["values"]:
            tmpRow: dict[str, str] = dict()
            # Iterate over columns
            for field in fields:
                col_id = field["col"]
                col_name = field["name"]
                tmpRow[col_name] = rowData[col_id] if col_id < len(rowData) else ""
            # Save row
            self.combinedData.append(tmpRow)
        # Get date of last update and generation
        self.docName = (
            self.service_drive.files()
            .get(fileId=self.spreadsheetId, fields="name, modifiedTime")
            .execute()
            .get("name")
        )
        modifiedTime = (
            self.service_drive.files()
            .get(fileId=self.spreadsheetId, fields="name, modifiedTime")
            .execute()
            .get("modifiedTime")
        )
        modifiedTimeParsed = parser.parse(modifiedTime)
        self.lastUpdateDate = modifiedTimeParsed.strftime("%d.%m.%Y %H:%M:%S")
        self.generationDate = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        # Write details to log
        logger.info(f"File name: {self.docName}")
        logger.info(f"Last update: {self.lastUpdateDate}")
        logger.info(f"Generation date: {self.generationDate}")
        logger.info(f"Lines count: {len(self.combinedData)}")

    def saveToFile(self, filename):
        logger.info("Saving to file...")
        with open(filename, "w") as f:
            f.write("var php_data = ")
            f.write(
                json.dumps(self.combinedData, separators=(",", ":"), ensure_ascii=False)
            )
            f.write(";\n")
            f.write(f'var modified_date="{self.lastUpdateDate}";\n')
            f.write(f'var generated_date="{self.generationDate}";\n')
        logger.info(f"Saved to file: {filename}")
