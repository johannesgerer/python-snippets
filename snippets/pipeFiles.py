import pandas as pd
import zipfile
import re
from pathlib import Path
import io


def extractZippedFiles(archive, regex=None):
    with zipfile.ZipFile(archive) as z:
        return [(x, io.TextIOWrapper(z.open(x)).read()) for x in z.namelist()
                if regex is None or not re.search(regex, x) is None]

def getUniqueTableNames(tables):
    """
    tables: [(filename, tableName, qualifiedTableName, DataFrame)]

    returns: dict(tableName or if needed qualifiedTableName, DataFrame)
    """
    tables = [x for x in tables if x[3] is not None]
    tableNames = [x for _,x,_,_ in tables]
    qualifiedTableNames = [x for _,_,x,_ in tables]
    duplicates = [(fn,tn,qtn, len(df)) for fn,tn,qtn,df in tables if qualifiedTableNames.count(qtn)>1]
    assert len(duplicates)==0, "Duplicate tables\n"+"\n".join([str(x) for x in duplicates])
    return {(qtn if tableNames.count(tn)>1 else tn):df for _,tn,qtn,df in tables}


def extractPipeColumnTypes(header):
    dateColumns = []
    dtypes = {}
    for _, col in header.iterrows():
        if   col[2]=='N': dtypes[col[1]] = 'Int64' if col[3] == 0 else 'float64'
        elif col[2]=='D': dateColumns.append(col[1])
        elif col[2]=='S': dtypes[col[1]] = 'str'
    return ['dropped'] + list(header.iloc[:,1]), dtypes, dateColumns
                       
    
def extractTablesFromPipeFile(fileName, fileText, regex=None, debug=False):
    """
    Get sample file from: https://www.msci.com/zh/sample-files
    """
    result = []
    for section  in fileText.split('#EOD\n*\n')[:-1]:
        headerText, tableText = section.split('*\n')[1:3]

        header = pd.read_fwf(io.StringIO(headerText), colspecs=[(5,39), (39,70), (70,71), (76,78)])

        def printDebugInfo():
            n=200
            print('fileName:', fileName, f'\nsection[:{n}]:\n', section[:n], '\nheader:\n', header)

        tableName = header.columns[0]

        if not regex is None and re.search(regex, tableName) is None:
            result.append((fileName, tableName, tableName + ' ' + header.columns[1], None))
            continue

        if debug: printDebugInfo()
        try:
            (columns, dtypes, dateColumns) = extractPipeColumnTypes(header)
            if debug: print(dtypes)
            df = pd.read_csv(io.StringIO(re.sub(' *\\| *','|', tableText)), sep='|', index_col=False,
                             na_values=["NULL",""], keep_default_na=False,
                             names=columns, usecols = list(range(1,len(columns))),
                             date_parser = lambda x: pd.to_datetime(x, format="%Y%m%d"),
                             dtype=dtypes, parse_dates=dateColumns, skiprows=2)
        except  ValueError as v: 
            printDebugInfo()
            raise v

        if debug: print(df.dtypes)
        result.append((fileName, tableName, tableName + ' ' + header.columns[1], df))

    return result
    

def extractTablesFromPipeFileOrZip(path, fileRegex=None, tableRegex=None, debug=False):
    results = []
    
    for x in extractZippedFiles(path, regex=fileRegex) if zipfile.is_zipfile(path) \
        else [(path, Path(path).read_text())]:
        results += extractTablesFromPipeFile(*x, regex=tableRegex, debug=debug)

    if debug:
        for a,b,c,d in results:
            print({"file": a, "tableName": b, "tableNameExtra": c, "df.shape": None if d is None else d.shape})
    return getUniqueTableNames(results)
