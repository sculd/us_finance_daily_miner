# Use an official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.7-slim

# Install production dependencies.
RUN pip install pandas numpy
RUN pip install Flask gunicorn
RUN pip install RequestsThrottler
RUN pip install tornado
RUN pip install google-cloud-bigquery==1.27.2
RUN pip install google-cloud-bigquery-storage==1.0.0
RUN pip install --upgrade google-cloud-bigquery[bqstorage,pandas]
RUN pip install sendgrid
RUN pip install google-cloud-core
RUN pip install python-dateutil
RUN pip install pydata-google-auth
RUN pip install finnhub-python
RUN pip install polygon-api-client
RUN pip install simfin
RUN pip install scikit-learn


# Copy local code to the container image.
# WORKDIR /sgen-app
COPY . .

# Service must listen to $PORT environment variable.
# This default value facilitates local development.
ENV PORT 8080
ENV ENVAR_URL http://apigee-pinpoint-prod.apigee.net/v1/default
ENV ENVAR_RUNTIME 1
ENV ENVAR_DELAY 0 
ENV ENVAR_BURSTS 1
ENV ENVAR_ASYNC  1

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 app:app

