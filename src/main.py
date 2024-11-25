from converters import table_converter

credentials = 'credentials/credentials.json'
spreadsheetId = '1bkbojWWjPXLkOXGd6yZRqDVlsyXurWxL3ePQzvTM3Jc'
spreadsheetRange = 'Сводный!A2:M'


if __name__ == '__main__':
    converter = table_converter.TableConverter(credentials)
    converter.auth()
    converter.setSpreadsheetId(spreadsheetId)
    converter.setSpreadsheetRange(spreadsheetRange)
    converter.saveToFile('output/table.json')
