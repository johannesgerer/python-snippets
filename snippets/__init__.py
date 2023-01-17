import pandas as pd

def toExcel(dfs, target, 
            autoFilter=True,
            index=False,
            maximumColumnWidth=None,
            ignoreHeaderWidth=True,
            headerFormat={'align':'left', 'bold':True},
            ):
    """
    dfs: either single dataframe
         or a dictionary mapping sheetnames to dataframes or to pais of
                (dataframe, Dictionary from column names to pairs of columns format overrides and widts)
    """
    dfs = {"Sheet1": dfs} if type(dfs) != dict else dfs

    excelWriter = pd.ExcelWriter(target, engine='xlsxwriter')

    if headerFormat is not None:
        headerFormat = excelWriter.book.add_format(headerFormat)

    for sname, data in dfs.items():
        formats = {}
        if type(data) == tuple:
            data, formats = data

        dataWithPotentialIndex = data.reset_index() if index else data
        nRows,nCols = dataWithPotentialIndex.shape
        print(f"Writing {sname} with {nRows} lines and {nCols} cols to", target, end=" ... ")

        data.to_excel(excelWriter, sname, index=index)

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
