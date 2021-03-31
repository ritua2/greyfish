##########################
# BASICS
# 
# A Debian server with python3 installed, useful only as a cloud storage server
# Not designed for front-end interfaces
#########################

FROM python:3.8.2-buster


# Copies the necessary files
COPY API_Daemon.sh          /grey/API_Daemon.sh
COPY server.py              /grey/server.py
COPY templates/index.html   /grey/templates/index.html
COPY certfile.crt           /grey/certfile.crt
COPY keyfile.key            /grey/keyfile.key

# MANAGER NODE IP
ENV MANAGER_NODE_IP "IP_ADDRESS_OF_MANAGER_NODE"
# ACCESS NODE IP
ENV ACCESS_NODE_IP "IP_ADDRESS_OF_ACCESS_NODE"
# number of threads to run
ENV greyfish_threads "8"




# Installs the necessary packages
# Bash for convenience
RUN pip3 install gunicorn Flask requests &&\
    chmod +x /grey/server.py /grey/API_Daemon.sh


WORKDIR /grey
CMD tail -F anything
