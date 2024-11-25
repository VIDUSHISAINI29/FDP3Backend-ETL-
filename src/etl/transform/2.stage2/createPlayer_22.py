import duckdb
connection = duckdb.connect(":memory:")

outputParquetPath = 'D:/VS Code/FullStackDashboardsProject/FullStackDashboardProject-3/FDP3Backend-ETL/data/transformed/2.stage2/Players_Data_22.parquet'

connection.execute(f"""
                   CREATE TABLE Players_Data AS SELECT * FROM read_parquet('data/transformed/1.stage1/Fifa_22.parquet')
                   """)
query = f"""SELECT pd.ID, pd.Name, pd.Age, pd.Photo, pd.Nationality, pd.Flag, pd.Club, pd."Jersey Number", pd.Joined, pd.SprintSpeed, pd.Agility, pd.Balance, pd.ShotPower, pd.Jumping, pd.Stamina, pd.Volleys, pd.Height, pd.Weight FROM Players_Data pd"""
connection.execute(
    f"""
    COPY ({query}) TO '{outputParquetPath}' (FORMAT PARQUET)
    """
)

print ("Fifa_22 Stage 2 Done !! 💖")