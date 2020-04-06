import os
import re
import csv
from bs4 import BeautifulSoup
from lxml import etree

def findabsentfields(knownFieldList, jsonFilesLocation):
    jsonFileList = os.listdir(jsonFilesLocation)
    fieldsPresent = []
    fieldsAbsent = []
    for file in jsonFileList:
        jsonString = open("json-file-source-path" + file, 'rt', encoding='utf8').read()
        for field in knownFieldList:
            if field in jsonString:
                if field not in fieldsPresent:
                    fieldsPresent.append(field)
            else:
                if field not in fieldsAbsent:
                    fieldsAbsent.append(field)
    return(fieldsPresent, fieldsAbsent)


def createTableStatement(tableName, schemaName, databaseName, dataTypeDict, tableType='', isReplaceRequired=False):
    if tableType == '':
        if(isReplaceRequired == False):
            createSQL = 'create table ' + makeTableName(databaseName,schemaName,tableName)
        else:
            createSQL = 'create or replace table ' + makeTableName(databaseName,schemaName,tableName)

        createSQL += ' ('
        for item in dataTypeDict:
            createSQL += item + ' ' + dataTypeDict[item] + ','
        createSQL = createSQL.rstrip(',')
        createSQL += ')'

    return(createSQL)


def copyIntoRAW(tableName, schemaName, databaseName, stageName):
    createStatement = createTableStatement(tableName=tableName+'_document', schemaName=schemaName, databaseName=databaseName, dataTypeDict={"document":"variant", "record_captured_at":"timestamp_ntz", "blob_file_name":"varchar"})
    copyIntoStatement = 'copy into ' + databaseName + '.' + schemaName + '.' + tableName + '_document from (select $1, convert_timezone(\'UTC\', current_timestamp(2))::timestamp_ntz as record_captured_at, metadata$filename as blob_file_name from @' + stageName + ')'
    return createStatement, copyIntoStatement


def makeTableName(databaseName, schemaName, tableName):
    newTableName = databaseName + '.' + schemaName + '.' + tableName
    return newTableName


def makeViewName(databaseName, schemaName, viewName):
    newViewName = databaseName + '.' + schemaName + '.' + viewName
    return newViewName


def createStoredProcedure(storedProcedureName, inputSQL, procedureReturns):
    storedProcedure = "create or replace procedure " + storedProcedureName + "()\nreturns " + procedureReturns + "\nlanguage javascript\nas\n$$\n"
    for eachSQL in inputSQL:
        sqlIndex = str(inputSQL.index(eachSQL))
        spStatement = 'var sql_stmt_' + sqlIndex + ' = "' + eachSQL + '";\n'
        spExeStatement = 'var exec_stmt_' + sqlIndex + ' = snowflake.createStatement({sqlText:sql_stmt_' + sqlIndex + '});\n'
        spExecute = "exec_stmt_" + sqlIndex + ".execute();\n\n"
        storedProcedure = storedProcedure + spStatement + spExeStatement + spExecute
    storedProcedure += "$$"
    return storedProcedure


def createView(viewName, schemaName, databaseName, dataTypeDict, sourceColDict, fromSource, miscInfo):
    createSQL = 'create view ' + makeViewName(databaseName, schemaName, viewName)
    createSQL += ' as (select '
    for item in dataTypeDict:
        createSQL += sourceColDict[item] + '::' + dataTypeDict[item] + ' as ' + item + ','
    createSQL = createSQL.rstrip(',')
    if(miscInfo!=''):
        createSQL += ' from ' + fromSource + ', ' + miscInfo + ')'
    else:
        createSQL += ' from ' + fromSource + ')'
    return createSQL


def getColumnInfo(csvLocation):
    inputCSV = open(csvLocation, 'rt', encoding='utf-8')
    csvReader = csv.reader(inputCSV)
    sourceColDict = {rows[0]:rows[1] for rows in csvReader}
    inputCSV = open(csvLocation, 'rt', encoding='utf-8')
    csvReader = csv.reader(inputCSV)
    dataTypeDict = {rows[0]:rows[2] for rows in csvReader}
    return sourceColDict, dataTypeDict


def convertToSnakecase(camelCaseInput):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camelCaseInput)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def convertToCamelCase(snakeCaseInput):

    splitSnake = snakeCaseInput.split("_")
    outputcamelstring = ''
    for eachpart in splitSnake:
        outputcamelstring += eachpart.title()
    return outputcamelstring


def findWordInFiles(word, filesLocation):
    files = []
    jsonFileList = os.listdir(filesLocation)
    for file in jsonFileList:
        jsonString = open(
            filesLocation + '\\' + file, 'rt',
            encoding='utf8').read()
        if word.upper() in jsonString.upper():
            files.append(file)
    return(files)


def traceJSONpath(JSON, path, all_paths):
    all_paths.append(path.rstrip('.'))
    for key in JSON:
        if isinstance(JSON[key], list):
            for eachItem in JSON[key]:
                if isinstance(eachItem, dict):
                    traceJSONpath(eachItem, path + key + '.', all_paths)
        else:
            if isinstance(JSON[key], dict):
                traceJSONpath(JSON[key], path + key + '.', all_paths)


def findelementinJSON(element, JSON, path, all_paths):
    if element in JSON:
        if path=='':
            path = 'document'
        all_paths.append(path.rstrip('.'))
    for key in JSON:
        if isinstance(JSON[key], list):
            for eachItem in JSON[key]:
                if isinstance(eachItem, dict):
                    findelementinJSON(element, eachItem, path + key + '.', all_paths)
        else:
            if isinstance(JSON[key], dict):
                findelementinJSON(element, JSON[key], path + key + '.', all_paths)


def generatedocpage(modelInfoFile):
    modelName = modelInfoFile.split('\\')[-1].split('.')[0]
    tab2 = '  '
    tab4 = '    '
    colList = []
    inputCSV = open(modelInfoFile, 'rt', encoding='utf-8')
    csvReader = csv.reader(inputCSV)
    colInfoDict = {rows[0]: rows[1] for rows in csvReader}
    for key in colInfoDict:
        colList.append(key)

    colList.remove('Column')

    for column in colList:
        if(column == 'Model Description'):
            print(tab2 + ' - name: ' + modelName)
            print(tab2 + '   description: asfdfadf')
            print()
            print(tab4 + ' ' + 'columns:')
        else:
            print(tab4 + tab4 + ' - name: ' + column)
            print(tab4 + tab4 + '   description: ' + '\'{{doc("'+ column.lower() + '")}}\'')
            print()


def generatemdfile(attributesLocation, dbtProjectLocation):
    # this works only if your code is based on a HTML page
    filesInLocation = os.listdir(attributesLocation)
    columnLongNameDict = {}
    columnSnakeNameDict = {}
    columnNameDefDict = {}
    for eachfile in filesInLocation:
        print(eachfile)
        html1 = open(attributesLocation + '\\' + eachfile, 'r')
        soup = BeautifulSoup(html1, 'lxml')
        table = soup.find_all('table')

        for eachtable in table:
            tree = etree.fromstring(str(eachtable))
            attributeNameHeader = tree.xpath("//tr[2]/td[1]/b/font/text()")
            attributeNameContent = tree.xpath("//tr[2]/td[2]/font/text()")

            columnNameHeader = tree.xpath("//tr[3]/td[1]/b/font/text()")
            columnNameContent = tree.xpath("//tr[3]/td[2]/font/text()")

            columnDefinitionHeader = tree.xpath("//tr[8]/td[1]/b/font/text()")
            columnDefinitionContent = tree.xpath("//tr[8]/td[2]/font/pre/font/text()")

            logicalRolenameHeader = tree.xpath("//tr[4]/td[1]/b/font/text()")
            logicalRolenameContent = tree.xpath("//tr[4]/td[2]/font/text()")

            if len(logicalRolenameContent) > 0:
                logicalRolenameContent = logicalRolenameContent[0].rstrip('\\xa0').rstrip()
                attributeNameHeader = attributeNameHeader[0].rstrip('\\xa0').rstrip()
                columnNameContent = columnNameContent[0].rstrip('\\xa0').rstrip()
                columnNameHeader = columnNameHeader[0].rstrip('\\xa0').rstrip()
                columnLongName = ''

                if logicalRolenameContent != '' and logicalRolenameHeader == 'Logical Rolename':
                    columnLongName = logicalRolenameContent
                elif attributeNameHeader == 'Attribute Name':
                    columnLongName = attributeNameContent[0].rstrip('\\xa0').rstrip()

                if (columnNameHeader == 'ColumnName'):
                    columnName = columnNameContent
                    columnLongNameDict[columnName] = columnLongName
                    columnSnakeNameDict[columnName] = convertToSnakecase(columnName)
                    if len(columnDefinitionHeader) > 0:
                        columnDefinitionContent = ''.join(columnDefinitionContent).rstrip('\\xa0').rstrip()
                    columnNameDefDict[columnName] = columnDefinitionContent

    outputmdFile = open(dbtProjectLocation + '\\models\\columns.md', 'w')
    # csvwriter = csv.writer(open(folderLocation + 'output.csv', 'w', newline=''))
    for key in columnLongNameDict:
        outputmdFile.write('{% docs ' + columnSnakeNameDict[key] + ' %}\n')
        outputmdFile.write(columnNameDefDict[key].replace('date_time', 'datetime') + '\n')
        outputmdFile.write('{% enddocs %}\n')
        outputmdFile.write('\n')

        outputmdFile.write('{% docs ' + key.lower() + ' %}\n')
        outputmdFile.write(columnNameDefDict[key] + '\n')
        outputmdFile.write('{% enddocs %}\n')
        outputmdFile.write('\n')

        # csvwriter.writerow([columnLongNameDict[key], key, columnSnakeNameDict[key], columnNameDefDict[key]])


    outputmdFile.close()


def generateTablemdfile(attribpath, dbtproj):
    # this works only if your code is based on a HTML page
    filesInLocation = os.listdir(attribpath)
    tableDict = {}
    for eachfile in filesInLocation:
        # print(eachfile)
        html1 = open(attribpath + '\\' + eachfile, 'r')
        soup = BeautifulSoup(html1, 'lxml')
        table = soup.find_all('table')

        for eachtable in table:
            tree = etree.fromstring(str(eachtable))
            tableDescHeader = tree.xpath("//tr[6]/td[1]/b/font/text()")
            tableDescContent = tree.xpath("//tr[6]/td[2]/font/pre/font/text()")

            tableNameHeader = tree.xpath("//tr[2]/td[1]/b/font/text()")
            tableNameContent = tree.xpath("//tr[2]/td[2]/font/text()")

            tableNameHeader2 = tree.xpath("//tr[3]/td[1]/b/font/text()")
            tableNameContent2 = tree.xpath("//tr[3]/td[2]/font/text()")

            if len(tableDescHeader) > 0:
                tableName = ''
                description = ''
                if tableNameHeader[0].find("Default Table Name") > -1:
                    tableName = tableNameContent[0].rstrip('\xa0')
                else:
                    tableName = tableNameContent2[0].rstrip('\xa0')

                if len(tableDescContent) > 0:
                    description = tableDescContent[0].rstrip()
                else:
                    description = 'Not available'

                if tableDescHeader[0].find('Definition') > -1:
                    tableDict[tableName] = description

    outputmdFile = open(dbtproj + '\\models\\tables.md', 'w')
    for key in tableDict:
        outputmdFile.write('{% docs ' + convertToSnakecase(key) + ' %}\n')
        outputmdFile.write(
            tableDict[key].replace('date_time', 'datetime') + '\n')
        outputmdFile.write('{% enddocs %}\n')
        outputmdFile.write('\n')


    outputmdFile.close()