import os
import mlflow
import pandas as pd
from flask import Flask, request, jsonify

RUN_ID = os.environ.get("RUN_ID")
EXPERIMENT_ID = os.environ.get("EXPERIMENT_ID")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

if None in (RUN_ID, EXPERIMENT_ID, S3_BUCKET_NAME):
    raise ValueError("AWS configuration is not set in the environment variables")

# load model from S3
model_url = f"s3://{S3_BUCKET_NAME}/{EXPERIMENT_ID}/{RUN_ID}/artifacts/models"
model = mlflow.pyfunc.load_model(model_url)


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


# create the flask app
bixi_app = Flask('bixi-ride-duration-prediction')

# create the prediciton endpoint
@bixi_app.route('/predict', methods=['POST'])
def duration_prediction():

    trip_details = request.get_json()
    duration = predict(trip_details)

    prediction = {
        'duration_minute': duration,
        'model_version': RUN_ID
    }

    return jsonify(prediction)

# start the flask app
if __name__ == "__main__":
    bixi_app.run(debug=True, host='0.0.0.0', port=9696)
