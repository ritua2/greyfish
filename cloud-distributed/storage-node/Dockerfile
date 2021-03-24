##########################
# BASICS
# 
# A Debian server with python3 installed, useful only as a cloud storage server
# Not designed for front-end interfaces
#########################

FROM python:3.8.2-buster


# Copies the necessary files
COPY API_Daemon.sh      /grey/API_Daemon.sh
COPY base_functions.py  /grey/base_functions.py
COPY traffic.py         /grey/traffic.py
COPY setup.sh           /grey/setup.sh
COPY certfile.crt       /grey/certfile.crt
COPY keyfile.key        /grey/keyfile.key

# Individual key used to attach node or to give it instructions
ENV NODE_KEY "node1"
# Similar to that of the manager node
ENV orchestra_key "karaoke"
# Similar to that of the manager node
ENV URL_BASE "example.com"
# Similar to that of the manager node
ENV MYSQL_DATABASE "greyfish"
# Similar to that of the manager node
ENV MYSQL_USER "root"
# Similar to that of the manager node
ENV MYSQL_PASSWORD "password"
# Filesystem where all data will stored. For example: /dev/sda1
ENV FILESYSTEM "overlay"
# Maximum total storage allowed for users in KB, must be a positive integer
ENV MAX_STORAGE "1000000"
# Reserved space for compressed files while downloading
ENV RESERVED_STORAGE "300000"
# number of threads to run
ENV greyfish_threads "4"




# Installs the necessary packages
# Bash for convenience
RUN mkdir -p /greyfish/sandbox &&\
    pip3 install gunicorn Flask mysql-connector-python requests &&\
    chmod +x /grey/setup.sh /grey/traffic.py /grey/API_Daemon.sh


WORKDIR /greyfish
CMD /grey/setup.sh
