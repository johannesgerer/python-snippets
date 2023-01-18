import pandas as pd

def toExcel(dfs, target, 
            autoFilter=True,
            index=False,
            maximumColumnWidth=None,
            ignoreHeaderWidth=True,
            headerFormat={'align':'left', 'bold':True},
            engine='xlsxwriter' # or openpyxl, but this does not support what this function does, e.g. autofilter, width, etc 
            ):
    """
    dfs: either single dataframe
         or a dictionary mapping sheetnames to dataframes or to pais of
                (dataframe, Dictionary from column names to pairs of columns format overrides and widts)
    """
    dfs = {"Sheet1": dfs} if type(dfs) != dict else dfs

    excelWriter = pd.ExcelWriter(target, engine=engine)

    if headerFormat is not None:
        headerFormat = excelWriter.book.add_format(headerFormat)

    for sname, data in dfs.items():
        formats = {}
        if type(data) == tuple:
            data, formats = data

        dataWithPotentialIndex = data.reset_index() if index else data
        nRows,nCols = dataWithPotentialIndex.shape
        print(f"Writing {sname} with {nRows} lines and {nCols} cols to", target, end=" ... ")

        data.to_excel(excelWriter, sheet_name=sname, index=index)

        worksheet = excelWriter.sheets[sname]

        for columnIndex, columnName in enumerate(dataWithPotentialIndex):
            columnWidth = dataWithPotentialIndex.iloc[:, columnIndex].astype(str).str.len().max()
            columnWidth = max(columnWidth, autoFilter + (not ignoreHeaderWidth)*len(str(columnName)))
            if not maximumColumnWidth is None: columnWidth = min(maxWidth, columnWidth)

            optional = {}

            colFormat, width = formats.get(columnName, ({}, None))

            if len(colFormat):
                optional = {'cell_format': excelWriter.book.add_format(colFormat)}

            worksheet.set_column(columnIndex, columnIndex,
                                 (columnWidth if width is None else width) * 1.2, **optional)

            if headerFormat is not None:
                worksheet.write(0, columnIndex, columnName, headerFormat)

        if autoFilter:
            worksheet.autofilter(0, 0, nRows, nCols - 1)

        print("Done")

    excelWriter.close()

def bloombergAdjustmentFactors(securityName, startDate, sizeAdjFactorOnly, includeQuotiens, df):
    """
    df: with columns
        ['Adjustment Date', 'Adjustment Factor', 'Adjustment Factor Operator Type',
        'Adjustment Factor Flag']

    return value: df with columns date [pandas datetime], sizeAdjFactor [float], priceAdjFactor [float]

    how to use the factors: multiply shares/prices with the forward filled factors to get comparable values


    this function does not support additive corrections.

    to minimize potential exceptions, set sizeAdjFactorOnly=True, if possible.
    """

    # bloomberg field def:
    # Column 1 - Adjustment Date
    # Column 2 - Adjustment Factor
    # Column 3 - Operator Type (1=div, 2=mult, 3=add. Opposite for Volume)
    # Column 4 - Flag (1=prices only, 3=prices and volumes

                          
    startDate = pd.to_datetime(startDate)

    columns = {'Adjustment Factor'                 :'factor',
               'Adjustment Factor Flag'            :'flag',
               'Adjustment Date'                   :'date',
               'Adjustment Factor Operator Type'   :'type'}

    df = pd.DataFrame(columns=list(columns.values())) if df.empty else df.rename(columns=columns)

    df['date']=pd.to_datetime(df.date)
    

    empty = pd.DataFrame({'date': [startDate], 'factor': [1]})

    df = df[df.date > startDate].sort_values('date', ignore_index=True)

    if sizeAdjFactorOnly:
        df = df[df.flag == 3]

    df = pd.concat([empty,df], ignore_index=True)

    unsupported = df[df.type==3]
    assert unsupported.empty, securityName + "\n" + str(unsupported)

    df.loc[df.type==2, 'factor'] = 1/df.factor

    df['sizeAdjFactor']                 = 1.0
    df.loc[df.flag==3, 'sizeAdjFactor'] = 1/df.factor

    if not sizeAdjFactorOnly:
        if includeQuotiens: 
            df['priceAdjFactorQuotient']= df.factor
        df['priceAdjFactor']            = df.factor.cumprod()

    if includeQuotiens: 
        df['sizeAdjFactorQuotient']     = df.sizeAdjFactor

    df['sizeAdjFactor']             = df.sizeAdjFactor.cumprod()
    
    return df.drop(columns=['type', 'flag', 'factor'])



# df = pd.DataFrame({'Adjustment Factor'                 :[],
#                             'Adjustment Factor Flag'            :[],
#                             'Adjustment Date'                   :[],
#                             'Adjustment Factor Operator Type'   :[]})

# bloombergAdjustmentFactors('test security1', "20230203", False, False, df)
