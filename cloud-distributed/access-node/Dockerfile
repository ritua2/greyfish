##########################
# BASICS
# 
# An alpine server with python3 installed, useful only as a cloud storage server
# Not designed for front-end interfaces
#########################

FROM python:3.8-alpine

# Copies the necessary files
COPY API_Daemon.sh      /grey/API_Daemon.sh
COPY base_functions.py  /grey/base_functions.py
COPY traffic.py         /grey/traffic.py
COPY checksums.py       /grey/checksums.py
#COPY certfile.crt       /grey/certfile.crt
#COPY keyfile.key        /grey/keyfile.key

# Docker environmental variables
# Greyfish values
ENV greyfish_key "clouddistributedgreyfish"
ENV greyfish_threads "8"
ENV orchestra_key "karaoke"

# Original URL or IP for the manager node (do not include ports or https://), i.e.: example.com
ENV URL_BASE "example.com"

# InfluxDB credentials
ENV INFLUXDB "db0"
ENV INFLUXDB_HTTP_AUTH_ENABLED "true"
ENV INFLUXDB_ADMIN_USER "admin"
ENV INFLUXDB_ADMIN_PASSWORD "admin"
ENV INFLUXDB_READ_USER "reader"
ENV INFLUXDB_READ_USER_PASSWORD "reader"
ENV INFLUXDB_WRITE_USER "writer"
ENV INFLUXDB_WRITE_USER_PASSWORD "writer"

# Mysql credentials
ENV MYSQL_DATABASE "greyfish"
ENV MYSQL_ROOT_PASSWORD "password"
ENV MYSQL_USER "greyuser"
ENV MYSQL_PASSWORD "password"

# Installs the necessary packages
# Bash for convenience
RUN apk update && apk add bash curl &&\
    pip3 install gunicorn Flask mysql-connector-python influxdb requests &&\
    chmod +x /grey/traffic.py /grey/API_Daemon.sh

WORKDIR /greyfish
CMD tail -F anything
