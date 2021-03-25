### Distributed Greyfish

--------------

At least, three machines are required for the installation of the distributed version of greyfish. There are three types of nodes:
* Manager node
* Access node
* Storage node

#### Installation (distributed)

* **Manager node**

Pull this directory and modify environmental variables:
```bash
git clone https://github.com/ritua2/greyfish.git
cd greyfish
git checkout distributed
cd cloud-distributed/manager-node

# Change the greyfish, mysql, InfluxDB credentials and other environmental variables
vi .env

# If mysql default credentials are updated in .env file, make respective changes in the start.sql file
vi initdb/start.sql
```

Obtain a certificate file (*.crt*) and private key (*.key*), make sure they are named *certfile.crt* and *keyfile.key*, respectively.  
If none are available, execute the following commands and answer the prompts as appropriate:
```bash
# Based on https://www.digitalocean.com/community/tutorials/openssl-essentials-working-with-ssl-certificates-private-keys-and-csrs
# Creates a certificate valid for 365 days
openssl req -newkey rsa:2048 -nodes -keyout keyfile.key -x509 -days 365 -out certfile.crt
```

Start manager node:
```bash
docker-compose up -d
```


Enter the manager node docker container and activate the APIs:  

```bash
# Enter container
docker exec -it managernode_greyfish_1 bash
cd /grey
# Start the needed databases and assign permission (APIs will not be started)
/grey/setup.sh
# Activate APIs(change the number of threads if needed, standard is 4)
./API_Daemon.sh -up
# To deactivate APIs(Ignore this command if setting up the manager node)
./API_Daemon.sh -down
```


To rebuild greyfish after changes in the manager node code:
```bash
# Bring down the containers
docker-compose down
# Build and start containers
docker-compose up -d --build
```



* **Access node**

Note: Can only be completed after installing the manager node and with the APIs and database containers being active


Pull this directory:
```bash
git clone https://github.com/ritua2/greyfish.git
cd greyfish
git checkout distributed
cd cloud-distributed/access-node
```

Obtain a certificate file (*.crt*) and private key (*.key*), make sure they are named *certfile.crt* and *keyfile.key*, respectively.  
If none are available, execute the following commands and answer the prompts as appropriate:
```bash
# Based on https://www.digitalocean.com/community/tutorials/openssl-essentials-working-with-ssl-certificates-private-keys-and-csrs
# Creates a certificate valid for 365 days
openssl req -newkey rsa:2048 -nodes -keyout keyfile.key -x509 -days 365 -out certfile.crt
```

Setup a access node, using the same *orchestra_key*, *greyfish_key*,  *Mysql Credentials*, *INFLUX Credentials* and *URL_BASE* environmental variables as the manager node. All the environmental variables are described in the [access node Dockerfile](./access-node/Dockerfile). Build the image by doing:
```bash
# Ensure all the environment variablles are set as instructed
vi Dockerfile
# Build the docker image
docker build -t greyfish/access-node:latest .
```


After building the image, activate the access node by running the container as follows:
```bash
# Start container
docker run --name accessnode_greyfish_1 -d -p 3443:3443 greyfish/access-node:latest
```


Enter the access node docker container and activate the APIs:  
```bash
# Enter container
docker exec -it accessnode_greyfish_1 bash
cd /grey
# Activate APIs(change the number of threads if needed, standard is 4)
./API_Daemon.sh -up
# To deactivate APIs(Ignore this command if setting up the access node)
./API_Daemon.sh -down
```
Note: deactivating the APIs will not change or delete any data, it will simply no longer be able to accept communications from outside.


* **Storage node**

Note: Can only be completed after installing the manager node & access node with the APIs and database containers being active


Pull this directory:
```bash
git clone https://github.com/ritua2/greyfish.git
cd greyfish
git checkout distributed
cd cloud-distributed/storage-node
```

Obtain a certificate file (*.crt*) and private key (*.key*), make sure they are named *certfile.crt* and *keyfile.key*, respectively.  
If none are available, execute the following commands and answer the prompts as appropriate:
```bash
# Based on https://www.digitalocean.com/community/tutorials/openssl-essentials-working-with-ssl-certificates-private-keys-and-csrs
# Creates a certificate valid for 365 days
openssl req -newkey rsa:2048 -nodes -keyout keyfile.key -x509 -days 365 -out certfile.crt
```

Setup a storage node, using the same *orchestra_key*, *Mysql Credentials* and *URL_BASE* environmental variables as the manager node. Other environmental variables are defined below:
* NODE_KEY: Individual key associated with each node
* FILESYSTEM: Filesystem where all data will stored. 'overlay', by default.
* MAX_STORAGE: Maximum total storage allowed for users in KB, must be a positive integer
* RESERVED_STORAGE: Reserved storage allowed for the compressed files for downloads in KB apart from MAX_STORAGE, must be a positive integer and less than MAX_STORAGE((1/3) * MAX_STORAGE recommended)

All the environmental variables are described in the [storage node Dockerfile](./storage-node/Dockerfile). Build the image by doing:
```bash
# Ensure all the environment variablles are set as instructed
vi Dockerfile
# Build the docker image
docker build -t greyfish/storage-node:latest .
```


After building the image, activate the storage node by running the container as follows:

```bash
# Start container
docker run --name storagenode_greyfish_1 -d -p 3443:3443 greyfish/storage-node:latest
```


Enter the access node docker container and activate the APIs:  
```bash
# Enter container
docker exec -it storagenode_greyfish_1 bash
cd /grey
# Activate APIs(change the number of threads if needed, standard is 4)
./API_Daemon.sh -up
# To deactivate APIs(Ignore this command if setting up the storage node)
./API_Daemon.sh -down
```


#### Data Persistance (distributed)



#### Usage (distributed)

The Greyfish APIs can be called from any system as long as the greyfish key is known. To simplify the usage of the greyfish a python script has been created which can run on any system which has python installed with following packages:
* requests
* tarfile


To access this script pull this directory:
```bash
git clone https://github.com/ritua2/greyfish.git
cd greyfish
git checkout distributed
cd cloud-distributed/speed-testing
vi pilot.py
```

The instructions on the usage of the script are defined inside script as comments. Also, a video has been recorded to demonstarte the usage of the script. 
Link to the video: https://youtu.be/LG430D0IpeU


The Greyfish APIs can be called from any system as long as the greyfish key is known.  
Note that, if creating a private key and certicate (as described above), the *--insecure* flag may be required for the *curl* commands.


```bash
gk=$Greyfish_Key # Set up in the docker-compose.yml
SERVER_IP=$URL_or_IP # Without ports or /
# Create a new user
curl -X POST -H "Content-Type: application/json" -d '{"gkey":"'"$gk"'", "user_id":"'"$USER_ID"'"}' \
	https://$SERVER_IP:2443/grey/create_user
# Delete a user
curl -X POST -H "Content-Type: application/json" -d '{"gkey":"'"$gk"'", "user_id":"'"$USER_ID"'"}' \
	https://$SERVER_IP:2443/grey/delete_user
```

Administrative and cluster actions:
```bash
# Remove storage node from cluster by IP as is (user data will not be redistributed among other storage nodes)
curl -X POST -H "Content-Type: application/json"\
    -d '{"orch_key":"karaoke", "node_IP":"111.111.11.11", "NODE_KEY":"node1"}' \
    --insecure https://"$SERVER_IP":2443/grey/cluster/removeme_as_is
```



#### Acknowledgements

All current Greyfish servers used for testing are setup using the Jetstream \[1\]\[2\] and Chameleon\[3\] systems. We are grateful to XSEDE for providing the allocation required for implementing this project. This project is generously supported through the National Science Foundation (NSF) award \#1664022.  




#### References

\[1\] Stewart, C.A., Cockerill, T.M., Foster, I., Hancock, D., Merchant, N., Skidmore, E., Stanzione, D., Taylor, J., Tuecke, S., Turner, G., Vaughn, M., and Gaffney, N.I., Jetstream: a self-provisioned, scalable science and engineering cloud environment. 2015, In Proceedings of the 2015 XSEDE Conference: Scientific Advancements Enabled by Enhanced Cyberinfrastructure. St. Louis, Missouri.  ACM: 2792774.  p. 1-8. http://dx.doi.org/10.1145/2792745.2792774 


\[2\] John Towns, Timothy Cockerill, Maytal Dahan, Ian Foster, Kelly Gaither, Andrew Grimshaw, Victor Hazlewood, Scott Lathrop, Dave Lifka, Gregory D. Peterson, Ralph Roskies, J. Ray Scott, Nancy Wilkins-Diehr, "XSEDE: Accelerating Scientific Discovery", Computing in Science & Engineering, vol.16, no. 5, pp. 62-74, Sept.-Oct. 2014, doi:10.1109/MCSE.2014.80


\[3\] Chameleon: a Scalable Production Testbed for Computer Science Research, K. Keahey, P. Riteau, D. Stanzione, T. Cockerill, J. Mambretti, P. Rad, P. Ruth,	book chapter in "Contemporary High Performance Computing: From Petascale toward Exascale, Volume 3",  Jeffrey Vetter ed., 2017 

