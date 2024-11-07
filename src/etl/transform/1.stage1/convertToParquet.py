import duckdb
connection = duckdb.connect(":memory:")

csvFilesArray = ['data/raw/FIFA17_official_data.csv', 'data/raw/FIFA18_official_data.csv', 'data/raw/FIFA19_official_data.csv', 'data/raw/FIFA20_official_data.csv', 'data/raw/FIFA21_official_data.csv', 'data/raw/FIFA22_official_data.csv', 'data/raw/FIFA23_official_data.csv' ]

tableNamesArray = ['Fifa_17', 'Fifa_18', 'Fifa_19','Fifa_20', 'Fifa_21', 'Fifa_22', 'Fifa_23']

outputParquetFilesPathArray = ['D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_17.parquet', 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_18.parquet', 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_19.parquet', 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_20.parquet', 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_21.parquet','D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_22.parquet', 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/1.stage1/Fifa_23.parquet']

for csvFile, tableName, outputParquetFilesPath in zip(csvFilesArray, tableNamesArray, outputParquetFilesPathArray):
    connection.execute(
        f"""
        CREATE TABLE {tableName} AS SELECT * FROM read_csv_auto('{csvFile}')
        """
    )
    query = f"""SELECT * FROM {tableName}"""
    connection.execute(
        f"""
        COPY ({query}) TO '{outputParquetFilesPath}'
        (FORMAT PARQUET)
        """
    )
    
    print ("Fifa Stage 1 Done !! ✨")