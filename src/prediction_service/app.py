import os
import mlflow
import requests
import logging 
from flask import Flask, request, jsonify
from pymongo import MongoClient


# configure logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler()]
)

RUN_ID = os.environ.get("RUN_ID")
EXPERIMENT_ID = os.environ.get("EXPERIMENT_ID")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

if None in (RUN_ID, EXPERIMENT_ID, S3_BUCKET_NAME):
    logging.info("AWS configuration is not set in the environment variables")
    raise ValueError("AWS configuration is not set in the environment variables")


# Env variables regarding monitoring service
EVIDENTLY_SERVICE_ADDRESS = os.environ.get('EVIDENTLY_SERVICE', 'http://127.0.0.1:5000')
MONGODB_ADDRESS = os.environ.get("MONGODB_ADDRESS", "mongodb://127.0.0.1:27017")

# load model from S3
logging.info("Loading model from the artifact store")
model_url = f"s3://{S3_BUCKET_NAME}/{EXPERIMENT_ID}/{RUN_ID}/artifacts/models"
model = mlflow.pyfunc.load_model(model_url)

def predict(features):
    preds = model.predict(features)
    return float(preds[0])


# create the flask app
bixi_app = Flask('bixi-ride-duration-prediction')

# initial the monitoring objects
logging.info("Configuring the MongoDB client")
mongo_client = MongoClient(MONGODB_ADDRESS)
db = mongo_client.get_database("prediction_service")
collection = db.get_collection("bixi_prediction")

# create the prediciton endpoint
@bixi_app.route('/predict', methods=['POST'])
def duration_prediction():

    trip_details = request.get_json()
    duration = predict(trip_details)

    logging.info("Sending request to the prediction service")
    prediction = {
        'duration_minute': duration,
        'model_version': RUN_ID
    }

    # upload the prediction for the monitoring servie
    logging.info("Sending prediction log to the MongoDB service")
    save_to_db(trip_details, float(duration))

    logging.info("Sending prediction log to the Evidently AI service")
    send_to_evidently_service(trip_details, float(duration))

    return jsonify(prediction)


def save_to_db(record, prediction):
    rec = record.copy()
    rec["duration_minute"] = prediction
    collection.insert_one(rec)


def send_to_evidently_service(record, prediction):
    rec = record.copy()
    rec["duration_minute"] = prediction
    requests.post(f"{EVIDENTLY_SERVICE_ADDRESS}/iterate/bixi", json=[rec])


# start the flask app
if __name__ == "__main__":
    bixi_app.run(debug=True, host='0.0.0.0', port=9696)
