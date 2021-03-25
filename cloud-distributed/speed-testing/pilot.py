"""
BASICS

Testing functions for distributed greyfish
"""


import requests, tarfile

def add_user(userid=None,gkey=None):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    data = {'user_id':userid,"gkey":gkey}
    r = requests.post("https://"+server_IP+":2443/grey/create_user", json = data, verify=False)
    print(r.text)

def remove_user(userid=None,gkey=None):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    data = {'user_id':userid,"gkey":gkey}
    r = requests.post("https://"+server_IP+":2443/grey/delete_user", json = data, verify=False)
    print(r.text)

def upload_file(userid=None,gkey=None,file=None,dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if file==None:
        print("Please enter the filepath")
        return
    
    files = {'file': open(file,'rb')}
    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload/"+gkey+"/"+userid,files=files, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload/"+gkey+"/"+userid+"/"+dir,files=files, verify=False)
    print(r.text)

def download_file(userid=None,gkey=None,file=None,ddir=None,dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if file==None:
        print("Please enter the filepath")
        return
    if ddir==None:
        print("Please enter the path for download directory with the filename to be saved")
        return
   
    if dir=='':
        r=requests.get("https://"+download_IP+":3443/grey/grey/"+gkey+"/"+userid+"/"+file, verify=False)
    else:
        r=requests.get("https://"+download_IP+":3443/grey/grey/"+gkey+"/"+userid+"/"+file+"/"+dir, verify=False)
    
    if "INVALID" in r.text:
        print(r.text)
    else:
        with open(ddir,'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

def delete_file(userid=None,gkey=None,file=None,dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if file==None:
        print("Please enter the filename")
        return
    
    if dir=='':
        r=requests.get("https://"+server_IP+":2443/grey/delete_file/"+gkey+"/"+userid+"/"+file, verify=False)
    else:
        r=requests.get("https://"+server_IP+":2443/grey/delete_file/"+gkey+"/"+userid+"/"+file+"/"+dir, verify=False)
    print(r.text)

def upload_dir(userid=None,gkey=None,file=None,dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if file==None:
        print("Please enter the filepath")
        return
    
    files = {'file': open(file,'rb')}
    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload_dir/"+gkey+"/"+userid,files=files, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload_dir/"+gkey+"/"+userid+"/"+dir,files=files, verify=False)
    print(r.text)

def upload_replace_dir(userid=None,gkey=None,file=None,dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if file==None:
        print("Please enter the filepath")
        return
    
    files = {'file': open(file,'rb')}
    if dir=='':
        r=requests.post("https://"+server_IP+":2443/grey/upload_replace_dir/"+gkey+"/"+userid,files=files, verify=False)
    else:
        r=requests.post("https://"+server_IP+":2443/grey/upload_replace_dir/"+gkey+"/"+userid+"/"+dir,files=files, verify=False)
    print(r.text)

def download_dir(userid=None,gkey=None, d_dir=None, dir=''):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if d_dir==None:
        print("Please enter the path for download directory with the filename to be saved")
        return
   
    if dir=='':
        r=requests.get("https://"+download_IP+":3443/grey/grey_dir/"+gkey+"/"+userid, verify=False)
    else:
        r=requests.get("https://"+download_IP+":3443/grey/grey_dir/"+gkey+"/"+userid+"/"+dir, verify=False)
    
    if "INVALID" in r.text:
        print(r.text)
    else:
        with open(d_dir,'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

def delete_dir(userid=None,gkey=None,dir=None):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    if dir==None:
        print("Please enter the complete path of directory to be deleted")
        return
    
    if dir=='':
        print("Please enter the directory")
        return
    else:
        r=requests.get("https://"+server_IP+":2443/grey/delete_dir/"+gkey+"/"+userid+"/"+dir, verify=False)
    print(r.text)

def view_user_dir(userid=None,gkey=None):
    if userid==None:
        print("Please enter the username")
        return
    if gkey==None:
        print("Please enter the greyfish key")
        return
    r=requests.get("https://"+download_IP+":3443/grey/grey_dir_json/"+gkey+"/"+userid, verify=False)
    print(r.text)

#####################
# IMPORTANT
#####################
# Update IP address variables with the IP address of manager-node and access-node from the cluster
# User must be added to the cluster to manage data on the cloud. So, add user first using add_user function from the list of storage functions listed after the variables.
# All the cloud based paths are seperated by '++' rather than '/' so if you want to define a path like dir1/dir2/dir3, it will be dir1++dir2++dir3 for cloud path. 
# Notes have been made next to each variable to guide with the path separator.
# Before using any function, update the variables passed as function arguments from below list.

#####################################
# Variables required for operations
#####################################

# id of user to perform operations
username="username"  

# greyfish key to access the cluster
greykey="clouddistributedgreyfish"

# IP of manager-node
server_IP = ""

# IP of access-node
download_IP = ""

# Entire path of the file on local system including it's name
upload_filename="/root/greyfish/cloud-distributed/speed-testing/essay_cpy.txt" # use /

# directory on the cloud where file will be uploaded, if left as "" the file will be uploaded to home directory of the user
# if the specified path doesn't exist on cloud, missing directories will be created for that path
cloud_dir_upload_filename="root++mydir" ## use ++

# Entire path of the compressed file containing folder to be uploaded, compressed file should be in .tar.gz or .tgz format
upload_dirname="/root/greyfish/cloud-distributed/speed-testing/testdir.tgz" # use /

# directory on the cloud where directory will be uploaded, if left as "" the directory will be uploaded to home directory of the user
# if the specified path doesn't exist on cloud, missing directories will be created for that path
cloud_dir_upload_dirname="root++mydir++myuploads" ## use ++

# name of the file only to be downloaded and it's respective directory on cloud
# if cloud directory is left as "", by default file will be searched in home directory for download
download_filename="essaay_cpy.txt" # use /
cloud_dir_download_filename="root++mydir++myuploads" # use ++

# Entire directory path of the directory to be downloaded from cloud
download_dirname="demo" # use ++

# file/directory download will be always in compressed file
# specify the whole path with .tar.gz extention where download will be saved.
save_path="/root/download/download.tar,gz" # use /

# name of the file only to be deleted and it's respective directory on cloud.
# if cloud directory is left as "", by default file will be searched in home directory for deletion
delete_filename="essay1.txt"
cloud_dir_delete_filename="root++mydir"   # use ++

# Entire directory path of the directory to be deleted from cloud
delete_dirname="root++mydir++myuploads" # use ++

################################
# Storage functions
################################

# NOTE:
# To perform an operation, uncomment the respective line below based on the operation and run the script using 'python3 pilot.py' command from cmd.
# After performing an operation comment out the line again to avoid conflicts unless want to run same operation with same or different parameters again.
# Ensure that all the function parameters are initialized as needed before running any function.
#################

# user add
#add_user(username,greykey)

# user remove
#remove_user(username,greykey)

# file upload
#upload_file(username, greykey, upload_filename) # will go to home directory
#upload_file(username, greykey, upload_filename, cloud_dir_upload_filename)

# file download
#download_file(username, greykey, download_filename, save_path) # will go to home directory
#download_file(username, greykey, download_filename, save_path, cloud_dir_download_filename)

# file delete
#delete_file(username, greykey, delete_filename) # will go to home directory
#delete_file(username, greykey, delete_filename, cloud_dir_delete_filename)

# directory upload
#upload_dir(username, greykey, upload_dirname) # will go to home directory
#upload_dir(username, greykey, upload_dirname, cloud_dir_upload_dirname)

# directory replace and upload
#upload_replace_dir(username, greykey, upload_dirname) # will go to home directory
#upload_replace_dir(username, greykey, upload_dirname, cloud_dir_upload_dirname)

# directory download 
#download_dir(username, greykey, save_path) # will go to home directory
#download_dir(username, greykey, save_path, download_dirname)

# directory delete 
#delete_dir(username, greykey, delete_dirname)

# directory contents for user
#view_user_dir(username, greykey)

