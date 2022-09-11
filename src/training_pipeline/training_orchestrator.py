from time import timezone
import mlflow
import os
import pandas as pd
import haversine as hs

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from datetime import timedelta


@task
def read_data(path: str, date_columns: list[int], header_col:int = 0) -> pd.DataFrame:
    return pd.read_csv(path, header=header_col, parse_dates=date_columns)

@task
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

@task
def generate_features(input_df: pd.DataFrame, target_column: str):
    
    feature_columns = input_df.columns.to_list()
    feature_columns.remove(target_column)
    # crate a data frame with train columns
    feature_df = input_df[feature_columns]
    
    # convert the data frame as a dictionary 
    feature_dict = feature_df.to_dict(orient='records')

    return feature_dict


@task
def build_model(X, y, alpha):
    # make skalearn pipeline
    pipeline = make_pipeline(
        DictVectorizer(),
        Lasso(alpha)
    )
    # initialize model
    pipeline.fit(X, y)
    # return the pipeline
    return pipeline

@flow
def train_and_register_model(train_ride_path: str, 
                             train_station_path: str,
                             valid_ride_path: str,
                             valid_station_path: str,
                             exp_name: str="bixi_ride_duration_prediction", 
                             developer_name: str="Mahmudul Hasan Bhuiyan"):
    
    # read train data
    print(f"Reading training rides data from: {train_ride_path}")
    train_ride_df = read_data(train_ride_path, [0, 2], 0)
    # sample a small portion of the rides to reduce runtime
    train_ride_df = train_ride_df.sample(frac=0.1, random_state=1, ignore_index=True)
    print(f"Length of training ride df: {len(train_ride_df)}")
    
    print(f"Reading training stations data from: {train_station_path}")
    train_stations_df = read_data(train_station_path, [], 0)
    print(f"Length of training stations df: {len(train_stations_df)}")
    
    # read validation data
    print(f"Reading validation rides data from: {valid_ride_path}")
    valid_ride_df = read_data(valid_ride_path, [0, 2], 0)
    # sample a small portion of the rides to reduce runtime
    valid_ride_df = valid_ride_df.sample(frac=0.1, random_state=1, ignore_index=True)
    print(f"Length of validation  ride df: {len(valid_ride_df)}")
    
    print(f"Reading validation stations data from: {valid_station_path}")
    valid_stations_df = read_data(valid_station_path, [], 0)
    print(f"Length of train stations df: {len(valid_stations_df)}")
    
    
    # preprocess data
    print("Preprocessing data")
    train_preprocessed_df = preprocess_data(train_ride_df, train_stations_df)
    print(f"Length of training preprocessed df: {len(train_preprocessed_df)}")
    
    valid_preprocessed_df = preprocess_data(valid_ride_df, valid_stations_df)
    print(f"Length of validation preprocessed df: {len(valid_preprocessed_df)}")
    
    
    # generate features
    print("Generating features")
    X_train = generate_features(input_df=train_preprocessed_df, target_column="duration_minute")
    y_train = train_preprocessed_df["duration_minute"].values

    X_val = generate_features(input_df=valid_preprocessed_df, target_column="duration_minute")
    y_val = valid_preprocessed_df["duration_minute"].values
    
    # set the experiment name
    mlflow.set_experiment(exp_name)
    
    with mlflow.start_run():
        
        mlflow.set_tag("developer", developer_name)
        # log the parameters in mlflow
        mlflow.log_param("train-ride-data-path", train_ride_path)
        mlflow.log_param("train-stations-data-path", train_station_path)
        mlflow.log_param("valid-ride-data-path", valid_ride_path)
        mlflow.log_param("valid-stations-data-path", valid_station_path)

        alpha = 0.1
        mlflow.log_param("alpha", alpha)
        
        print("Building the model")
        # build model
        model = build_model(X_train, y_train, alpha)
        
        # runnning prediction on the validation data
        print("Evaluating model on the validation data")
        y_pred = model.predict(X_val)
        
        rmse = mean_squared_error(y_val, y_pred, squared=False)
        mlflow.log_metric("rmse", rmse)
        print(f"RMSE on the validation data: {rmse}")
        
        # log the model and the dictvectorize
        mlflow.sklearn.log_model(model, artifact_path="models")
        print("Logged the model in artifacts")

@flow
def main_training_flow():

    # setup mlflow
    TRACKING_SERVER_HOST = os.environ.get("TRACKING_SERVER_HOST")
    if TRACKING_SERVER_HOST is None:
        raise ValueError("AWS configuration is not set in the environment variables")  
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")

    # Perform the training
    train_ride_path = "../../data/2022-05-01/20220105_donnees_ouvertes.csv"
    train_station_path = "../../data/2022-05-01/20220105_stations.csv"
    valid_ride_path = "../../data/2022-06-01/20220106_donnees_ouvertes.csv"
    valid_station_path = "../../data/2022-06-01/20220106_stations.csv"
    
    train_and_register_model(train_ride_path, train_station_path, valid_ride_path, valid_station_path)

# main_training_flow()

deployment = Deployment.build_from_flow(
    flow=main_training_flow,
    name="Bixi Model Training",
    schedule=CronSchedule(cron="0 0 5 * *", timezone="UTC"),
    work_queue_name="Bixi-Training-Queue"
)

deployment.apply()

    