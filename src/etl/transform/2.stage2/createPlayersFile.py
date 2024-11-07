import duckdb
connection = duckdb.connect(":memory:")

outputParquetPath = 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend(ETL)/data/transformed/2.stage2/Players_Data.parquet'

connection.execute(f"""
                   CREATE TABLE Players_Data AS SELECT * FROM read_parquet('data/transformed/1.stage1/Fifa_17.parquet')
                   """)
query = f"""SELECT pd.ID, pd.Name, pd.Age, pd.Photo, pd.Nationality, pd.Flag, pd.Club FROM Players_Data pd"""
connection.execute(
    f"""
    COPY ({query}) TO '{outputParquetPath}' (FORMAT PARQUET)
    """
)

print ("Fifa Stage 2 Done !! 🎉")