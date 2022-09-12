# Bixi Ride Duration Prediction Platform
MLOps project to predict ride duration using [Bixi bicycle ride data](https://bixi.com/en/open-data) of Montréal. This project is done as the capstone project of [MLOps Zoomcamp](https://github.com/DataTalksClub/mlops-zoomcamp) organized by [DataTalks.Club](https://datatalks.club/). 

# Project Overview

## Problem Definition
BIXI Montréal is a public bicycle sharing service operating in Montréal, Quebec, Canada. It started in 2009 and has become one of the largest bike-sharing systems in North America. In their BIXI application, they want to add a new feature that would give an estimation of the time needed for a ride given the start and the destination stations. This will help the users to make a better choice between the available transportation options. 

The BIXI application collects the data of different rides and the location data of stations. In the historical data, the duration of the rides already exists. The goal is to create an ML model to predict the duration of a ride using this historical ride data. 

## Proposed Solution
In this project, we propose a web service for BIXI that predicts the duration of a ride given the ride details. The BIXI application can call the endpoint of the web service with the ride details and show the prediction given by the service in the app. For developing the model, we collect the historical data available on the [Bixi Open Data](https://bixi.com/en/open-data) portal. They publish the data for the previous month at the beginning of each month. The service is containerized using Docker so that it can be easily deployed in the cloud and meet the scalability requirement of the application. 

Apart from the  model itself, we provide a training pipeline which is scheduled every month to train the model with the new data. The workflow is deployed in the cloud so that it is accessible and executable from a machine in local or cloud. This training pipeline registers the experiment metadata and the model in the cloud platform. The pipeline deployment and the execution detailes and logs can be visible from the cloud platform.

We also provide a real-time dashboard to monitor the model performance. The dashboard can detect drift in the features. The prediction model sends log to a data base. Further services such as reports could be developed on top of this data. 

To summrize, the proposed solution is composed of the following components. 

- A duration prediction service developed using Flask and containerized in Docker
- A model training pipeline in Prefect 2 deployed in AWS
- Model and metadata registry based on cloud deployment of MLFlow
- Realtime model performance monitoring dashboard using EvidentlyAI with capability of detecting data drift

## Tools and Technologies

**Programming Language:** Python. Flask for developing the Prediction and Monitoring services.

**Cloud Platform:** AWS. Used services are: AWS RDS, EC2, and S3. 

**Orchestration:** Prefect 2. Deployed in EC2 and stores the flows in the S3.

**Metadata and Model Registry:** MLFlow deployed in EC2. It uses AWS RDS (Postgres) for storing metadata and S3 for the artifacts. 

**Model Monitoring:** Evidently AI. Monitoring logs stored in Prometheus and dashboard is developed in Grafana.

**Containerization:** Docker

**Model prediction Logs:** MongoDB



Overall architecture of the project is shown below.

![Architecture of the Project](Images/project-architecture.png)
# Running the Project

## Environment Variables
For running the project, you need to setup the following environment variables.

- ** **
- ** **

