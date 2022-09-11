import os
from tkinter.tix import Tree
import pandas as pd
import haversine as hs

def read_data(path: str, date_columns: list[int], header_col:int = 0) -> pd.DataFrame:
    return pd.read_csv(path, header=header_col, parse_dates=date_columns)

def preprocess_data(ride_df: pd.DataFrame, station_df: pd.DataFrame)-> pd.DataFrame:
    
    # merge to get the start station details
    start_station_df = ride_df.merge(station_df, left_on="emplacement_pk_start", right_on="pk", how="inner")
    # rename the lattitude and longitude of the start station
    start_station_df.rename(columns={"latitude":"st_lattitude", "longitude":"st_longitude"}, inplace=True)
    # filter out the unneccesary columns
    start_station_df = start_station_df[["emplacement_pk_start", "emplacement_pk_end", "duration_sec", "is_member", "st_lattitude", "st_longitude"]]
    
    # merge to get the end stations detailes
    end_station_df = start_station_df.copy().merge(station_df, left_on="emplacement_pk_end", right_on="pk", how="inner")
    # rename the lattitude and longitude of the start station
    end_station_df.rename(columns={"latitude":"end_lattitude", "longitude":"end_longitude"}, inplace=True)
    # filter out the unneccesary columns
    end_station_df = end_station_df[["emplacement_pk_start", "emplacement_pk_end", "st_lattitude", "st_longitude", "end_lattitude", "end_longitude", "is_member", "duration_sec"]]
    
    # drop the rows with missing values
    end_station_df.dropna(inplace=True)
    
    # calculate the distance of the trip 
    end_station_df["distance_km"] = end_station_df.apply(lambda row: hs.haversine((row["st_lattitude"], row["st_longitude"]), (row["end_lattitude"], row["end_longitude"]), unit="km"), axis=1)
    
    # create a pair with start and end station
    end_station_df["ride_stations"] = end_station_df[["emplacement_pk_start", "emplacement_pk_end"]].astype(str).apply(lambda x: '_'.join(x), axis=1)
    # convert the duration to minute
    end_station_df["duration_minute"] = end_station_df["duration_sec"]/60
    
    # select the final columns
    processed_df = end_station_df[["ride_stations", "distance_km", "is_member", "duration_minute"]]
    # convert the categorical column to string
    processed_df["is_member"] = processed_df["is_member"].astype(str)
    
    return processed_df


# Perform the training
valid_ride_path = "../data/2022-06-01/20220106_donnees_ouvertes.csv"
valid_station_path = "../data/2022-06-01/20220106_stations.csv"

# read validation data
print(f"Reading validation rides data from: {valid_ride_path}")
valid_ride_df = read_data(valid_ride_path, [0, 2], 0)
# sample a small portion of the rides to reduce runtime
valid_ride_df = valid_ride_df.sample(frac=0.002, random_state=1, ignore_index=True)
print(f"Length of validation  ride df: {len(valid_ride_df)}")

print(f"Reading validation stations data from: {valid_station_path}")
valid_stations_df = read_data(valid_station_path, [], 0)
print(f"Length of train stations df: {len(valid_stations_df)}")

# preprocess data
valid_preprocessed_df = preprocess_data(valid_ride_df, valid_stations_df)
print(f"Length of validation preprocessed df: {len(valid_preprocessed_df)}")
print(valid_preprocessed_df.head(5))

# save files
save_path = "bixi_monitoring_06_22.csv"
valid_preprocessed_df.to_csv(save_path, sep=",", header=True, index=False)
save_path = "./evidently_service/datasets/bixi_monitoring_06_22.csv"
valid_preprocessed_df.to_csv(save_path, sep=",", header=True, index=False)
print("Saved files for monitoring")
    