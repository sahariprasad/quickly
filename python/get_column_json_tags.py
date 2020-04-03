import json
from functions import traceJSONpath

path2 = r"path-to-folder-containing-json-files"
eachfile = "testjson.json"


jsonFile = open(path2 + "\\" + eachfile, 'rt', encoding='utf-8')
j = json.load(jsonFile)
element = ''
all_paths = []

traceJSONpath(j, '', all_paths)
all_paths = set(all_paths)
columndict = {}

for path in all_paths:
    if '.' in path:
        outerpath = path.split('.')[0]
        innerpath = path.split('.')[-1]

        innerlist = (j[outerpath][0][innerpath])
        for item in innerlist:
            for key in item:
                if key in columndict:
                    columndict[key].append(path)
                else:
                    columndict[key] = [path]
    elif path == '':
        for key in j:
            if type(j[key]) is str:
                if key in columndict:
                    columndict[key].append('document')
                else:
                    columndict[key] = ['document']
    else:
        innerdict = j[path][0]
        for key in innerdict:
            if key in columndict:
                columndict[key].append(path)
            else:
                columndict[key] = [path]

for column in columndict:
    columndict[column] = set(columndict[column])

for column in columndict:
    print(column, columndict[column])