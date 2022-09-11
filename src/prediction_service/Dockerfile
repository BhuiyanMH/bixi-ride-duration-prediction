FROM python:3.9-slim

WORKDIR /app

COPY [ "requirements_prediction.txt",  "./" ]

RUN pip3 install -r requirements_prediction.txt

RUN pip3 install evidently

COPY [ "app.py", "./" ]

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=9696"]