### Distributed Greyfish: A scalable and portable file management system for Virtual Machines (VMs) on-prem and in the cloud 

Greyfish is a simple, out-of-the-box software for provisioning a multi-user, containerized filesystem in the cloud or on-prem. If you are building a web-application for which you need to support multiple users, having personal accounts on a shared storage, then Greyfish can be useful in this scenario. It helps in creating a data "vault" with appropriate access privileges for the users. You can also use Greyfish for creating your own Box/Google Drive /DropBox like applications. It provides the functionality for file-management - file/folder upload, file/folder download, and data persistence. The latest release of Greyfish supports distributed storage across multiple Virtual Machines (VMs) for scalability and load-balancing. As it is container-based, it can help in creating a portable service that can be started on or moved to any VM/system that supports Docker.

Powered with a WSGI server, Greyfish is multi-threaded. Data stored in Greyfish can be easily monitored using grafana or any other app.  


Greyfish allows single use tokens for specifc actions. These tokens are stored within a MySQL table, and can be accessed, created, or deleted from  another server or container within the same machine.


--------------

#### Installation (single node)

```bash
git clone https://github.com/ritua2/greyfish
cd greyfish
docker volume create --name=greyfish
# Change the influxdb log credentials
vi credentials.yml
# Set environment variables, appropriate passwords and base URL (without / and http(s)://
# Define the number of threads using "greyfish_threads", default is set to 4
vi .env
docker-compose up -d --build
```

To rebuild greyfish after changes in the code:
```bash
# Bring down the containers
docker-compose down
# Set the appropriate passwords and base URL (without / and http(s)://
# Define the number of threads using "greyfish_threads", default is set to 4
REDIS_AUTH="examplepass" URL_BASE=example.com greyfish_key="examplegrey" docker-compose up -d --build
```


Note: If using Docker for Mac, and want to run Greyfish on your localhost, set URL_BASE=docker.for.mac.localhost. When calling the Greyfish API, set SERVER_IP=localhost.


#### Instructions (single node)

To activate or switch off the APIs, enter the docker container and do:  

```bash
# Enter container
docker exec -it greyfish_greyfish_1 bash
cd /grey
# Start the needed databases and assign permission (APIs will not be started)
/grey/setup.sh
# Activate (change the number of threads if needed, standard is 4)
./API_Daemon.sh -up
# Deactivate
./API_Daemon.sh -down
```

Note: deactivating the APIs will not change or delete any data, it will simply no longer be able to accept communications from outside.


**Partial installations** (single node

* Installation without Redis temporary tokens: Set *redis_command* to a different Linux command.
* Installation without InfluxDB logs: Set *influx_command* to a a different Linux command.

Note: Greyfish can be setup without Redis and InfluxDB.


#### Data Persistance (single node)

Greyfish is setup using two Docker volumes by default, one storing the InfluxDB database with the log information, the other
containing the users' data (files and directories). These volumes ensure that, should the containers be brought the down,
the data will persist.
Furthermore, these volumes are shared with the host, so that other programs may use the data.  
If a user wishes to remove this functionality, simply remove the volume information from the *docker-compose.yml* file by
deleting lines 5, 6, 18, 19, 42, 43.



### Distributed setup

Installation, instructions, and usage available on [speed-testing](./cloud-distributed).



#### Usage (single node)

The Greyfish APIs can be called from any system as long as the greyfish key is known.  


```bash
gk=$Greyfish_Key # Set up in the docker-compose.yml

# Create a new user
curl http://$SERVER_IP:2003/grey/create_user/$gk/$USER_ID
# Delete a user
curl http://$SERVER_IP:2003/grey/delete_user/$gk/$USER_ID

# Get a JSON object of all user files
curl http://$SERVER_IP:2000/grey/all_user_files/$gk/$USER_ID
curl http://$SERVER_IP:2001/grey/all_user_files/$gk/$USER_ID

# Get contents of an entire directory in JSON (using ++ instead of / for paths)
curl http://$SERVER_IP:2000/grey/user_files/$gk/$USER_ID/PATH++TO++DIR
# Upload one file (will create the directory if needed
curl -F file=@$LOCAL_PATH_TO_FILE http://$SERVER_IP:2000/grey/upload/$gk/$USER_ID/PATH++TO++DIR
# Deletes a file
curl http://$SERVER_IP:2000/grey/delete_file/$gk/$USER_ID/$FILENAME/PATH++TO++DIR
# Deletes a directory (recursive)
curl http://$SERVER_IP:2000/grey/delete_dir/$gk/$USER_ID/PATH++TO++DIR
# Returns a file
curl http://$SERVER_IP:2000/grey/grey/$gk/$USER_ID/$FILENAME/PATH++TO++DIR
# Uploads a directory (must be compressed into .tgz or .tar.gz),
# if it already exists, it substitutes all files inside
curl -F file=@$LOCAL_PATH_TO_TAR http://$SERVER_IP:2000/grey/upload_dir/$gk/$USER_ID/PATH++TO++DIR
# Downloads a directory as a .tar.gz file
curl http://$SERVER_IP:2000/grey/grey_dir/$gk/$USER_ID/PATH++TO++DIR

# Gets all the data currently in the user directory
curl http://$SERVER_IP:2001/grey/get_all/$gk/$USER_ID
# Replaces all current data
curl -F file=@$TARRED_CONTENT  http://$SERVER_IP:2002/grey/push_all/$gk/$USER_ID

# Checksum actions
# Download a directory as a checksum file (first 8 characters of SHA256 checksum + tar.gz)
# This will move the tar file to a temporary checksum directory in case it needs to be checked later
# and delete the constituent directory files
# Both the -O and -J flags are required
curl -O -J http://$SERVER_IP:2000/grey/download_checksum_dir/$gk/$USER_ID/PATH++TO++DIR
# Or using wget:
wget --content-disposition http://$SERVER_IP:2000/grey/download_checksum_dir/$gk/$USER_ID/PATH++TO++DIR
# Delete a checksum file given its full name (i.e. FILENAME=y78t4jha.tar.gz) 
curl http://$SERVER_IP:2000/grey/delete_checksum_file/$gk/$USER_ID/$FILENAME


# Admin actions

# self_ID refers to how the admin wishes to refer to itself, useful in case of using temporary tokens
# Check all available usernames
curl -X POST -H "Content-Type: application/json" -d '{"key":"examplegrey", "self_ID":"admin1"}' http://$SERVER_IP:2004/grey/admin/users/usernames/all
# Purges all files older than Xsec seconds
curl -X POST -H "Content-Type: application/json" -d '{"key":"examplegrey", "self_ID":"admin1"}' http://$SERVER_IP:2004/grey/admin/purge/olderthan/$Xsec
``` 



#### Testing

The [speed-testing](./speed-testing) subdirectory contains a series of python scripts to test upload and download speeds for a Greyfish server.



#### Acknowledgements

All current Greyfish servers used for testing are setup using the Jetstream \[1\]\[2\] and Chameleon\[3\] systems. We are grateful to XSEDE for providing the allocation required for implementing this project. This project is generously supported through the National Science Foundation (NSF) award \#1664022.  

#### Cite As

Carlos Redondo and Ritu Arora. 2019. Greyfish: An Out-of-the-Box, Reusable, Portable Cloud Storage Service. In Proceedings of the Practice and Experience in Advanced Research Computing on Rise of the Machines (learning) (PEARC '19). Association for Computing Machinery, New York, NY, USA, Article 13, 1–6. DOI:https://doi.org/10.1145/3332186.3333055

#### References

\[1\] Stewart, C.A., Cockerill, T.M., Foster, I., Hancock, D., Merchant, N., Skidmore, E., Stanzione, D., Taylor, J., Tuecke, S., Turner, G., Vaughn, M., and Gaffney, N.I., Jetstream: a self-provisioned, scalable science and engineering cloud environment. 2015, In Proceedings of the 2015 XSEDE Conference: Scientific Advancements Enabled by Enhanced Cyberinfrastructure. St. Louis, Missouri.  ACM: 2792774.  p. 1-8. http://dx.doi.org/10.1145/2792745.2792774 


\[2\] John Towns, Timothy Cockerill, Maytal Dahan, Ian Foster, Kelly Gaither, Andrew Grimshaw, Victor Hazlewood, Scott Lathrop, Dave Lifka, Gregory D. Peterson, Ralph Roskies, J. Ray Scott, Nancy Wilkins-Diehr, "XSEDE: Accelerating Scientific Discovery", Computing in Science & Engineering, vol.16, no. 5, pp. 62-74, Sept.-Oct. 2014, doi:10.1109/MCSE.2014.80


\[3\] Chameleon: a Scalable Production Testbed for Computer Science Research, K. Keahey, P. Riteau, D. Stanzione, T. Cockerill, J. Mambretti, P. Rad, P. Ruth,	book chapter in "Contemporary High Performance Computing: From Petascale toward Exascale, Volume 3",  Jeffrey Vetter ed., 2017 

