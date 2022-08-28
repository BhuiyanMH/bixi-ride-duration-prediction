import pandas as pd

def read_data(path: str, date_columns: list[int], header_col:int = 0) -> pd.DataFrame:
    return pd.read_csv(path, header=header_col, parse_dates=date_columns)

def preprocess_df(input_df: pd.DataFrame)->pd.DataFrame:
    return input_df


ride_path = "data/2022-07-01/20220107_donnees_ouvertes.csv"
stations_path = "data/2022-07-01/20220107_stations.csv"

ride_df = read_data(ride_path, [0, 2], 0)
stations_df = read_data(stations_path, [], 0)

print(ride_df.head())
print(ride_df.dtypes)
print(stations_df.head())