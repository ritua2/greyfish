##########################
# BASICS
# 
# A Debian server with python3 installed, useful only as a cloud storage server
# Not designed for front-end interfaces
#########################

FROM python:3.8.2-buster


# Copies the necessary files
COPY certfile.crt       /grey/certfile.crt
COPY keyfile.key        /grey/keyfile.key
COPY setup.sh           /grey/setup.sh
COPY traffic.py         /grey/traffic.py

# Individual key used to attach node or to give it instructions
ENV NODE_KEY "node1"
# Similar to that of the manager node
ENV REDIS_AUTH "redis_auth"
# Similar to that of the manager node
ENV orchestra_key "karaoke"
# Similar to that of the manager node
ENV URL_BASE "example.com"
# Filesystem where all data will stored. For example: /dev/sda1
ENV FILESYSTEM "overlay"
# Maximum total storage allowed for users in KB, must be a positive integer
ENV MAX_STORAGE "1000000"




# Installs the necessary packages
# Bash for convenience
RUN mkdir -p /greyfish/sandbox &&\
    pip3 install gunicorn Flask redis requests &&\
    chmod +x /grey/setup.sh /grey/traffic.py


WORKDIR /greyfish
CMD /grey/setup.sh
