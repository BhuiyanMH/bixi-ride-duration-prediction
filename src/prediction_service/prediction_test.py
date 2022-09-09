import requests

trip_details = {
    "ride_stations": "9_394",
    "distance_km": 10,
    "is_member": "1"
}

url = 'http://localhost:9696/predict'
response = requests.post(url, json=trip_details)
print(response.json())