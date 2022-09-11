import json
from urllib import response
import uuid
import requests
import pandas as pd
from time import sleep




# table = pq.read_table("green_tripdata_2022-01.parquet")
# data = table.to_pylist()


# class DateTimeEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, datetime):
#             return o.isoformat()
#         return json.JSONEncoder.default(self, o)


# with open("target.csv", 'w') as f_target:
#     for row in data:
#         row['id'] = str(uuid.uuid4())
#         duration = (row['lpep_dropoff_datetime'] - row['lpep_pickup_datetime']).total_seconds() / 60
#         if duration != 0.0:
#             f_target.write(f"{row['id']},{duration}\n")
#         resp = requests.post("http://127.0.0.1:9696/predict",
#                              headers={"Content-Type": "application/json"},
#                              data=json.dumps(row, cls=DateTimeEncoder)).json()
#         print(f"prediction: {resp['duration']}")
#         sleep(0.01)

# read generate target for monotoring
ref_df = pd.read_csv("bixi_monitoring_06_22.csv", header=0)
target_df = []

for index, row in ref_df.iterrows():

    row = row.to_dict()
    row["id"] = str(uuid.uuid4())
    payload = {
        "ride_stations": str(row["ride_stations"]),
        "distance_km": row["distance_km"], 
        "is_member": str(row["is_member"]),
    }
    print("Sending data to prediction service")
    # send request to prediction service
    url = "http://127.0.0.1:9696/predict"
    response = requests.post(url, json=payload)
    print(response.json())
    # sleep(1)