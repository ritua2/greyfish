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
COPY certfile.crt       /grey/certfile.crt
COPY keyfile.key        /grey/keyfile.key
COPY setup.sh           /grey/setup.sh
COPY traffic.py         /grey/traffic.py




# Installs the necessary packages
# Bash for convenience
RUN apk update && apk add bash curl &&\
    pip3 install gunicorn Flask mysql-connector-python influxdb redis requests &&\
    chmod +x /grey/traffic.py /grey/API_Daemon.sh /grey/setup.sh


WORKDIR /greyfish
