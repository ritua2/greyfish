#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file
import os
import redis
import requests
import base_functions as bf
import mysql.connector as mysql_con
from werkzeug.utils import secure_filename
import tarfile, shutil
app = Flask(__name__)


URL_BASE = os.environ["URL_BASE"]
REDIS_AUTH = os.environ["REDIS_AUTH"]

GREYFISH_FOLDER = "/greyfish/sandbox/"
CURDIR = dir_path = os.path.dirname(os.path.realpath(__file__))
#################################
# USER ACTIONS
#################################



# Creates a user
@app.route("/grey/create_user", methods=['POST'])
def create_user():

    if not request.is_json:
        return "POST parameters could not be parsed"

    ppr = request.get_json()
    [error_occurs, missing_fields] = bf.error__l2_contains_l1(["user_id", "gkey"], ppr.keys())

    if error_occurs:
        return "INVALID: Lacking the following json fields to be read: "+missing_fields

    toktok = ppr["user_id"]
    gkey = ppr["gkey"]

    # Gets the IP address
    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        # Records all failed logins
        bf.failed_login(gkey, IP_addr, toktok, "create-new-user")
        return "INVALID key, cannot create a new user"

    user_action = bf.idb_writer('greyfish')

    # Stores usernames in Redis since this will be faster to check in the future
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select * from user where name=%s",(toktok,))
    uc=None
    for row in cursor:
        uc=row[0]
    cursor.close()
    grey_db.close()

    if uc != None:
        return "User already has an account"

    try:
        user_action.write_points([{
                            "measurement":"user_action",
                            "tags":{
                                    "id":toktok,
                                    "action":"signup"
                                    },
                            "time":bf.timformat(),
                            "fields":{
                                    "client-IP":IP_addr
                                    }
                            }])

        grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
        cursor = grey_db.cursor(buffered=True)
        cursor.execute("insert into user(name,max_data) values(%s,'100')",(toktok,))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "Greyfish cloud storage now available"
    except:
        return "INVALID, Server Error: Could not connect to database"


# Deletes an entire user directory
@app.route("/grey/delete_user", methods=['POST'])
def delete_user():

    if not request.is_json:
        return "POST parameters could not be parsed"

    ppr = request.get_json()
    [error_occurs, missing_fields] = bf.error__l2_contains_l1(["user_id", "gkey"], ppr.keys())

    if error_occurs:
        return "INVALID: Lacking the following json fields to be read: "+missing_fields

    toktok = ppr["user_id"]
    gkey = ppr["gkey"]

    IP_addr = request.environ['REMOTE_ADDR']

    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "delete-user")
        return "INVALID key, cannot delete user"

    user_action = bf.idb_writer('greyfish')

    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select * from user where name=%s",(toktok,))
    uc=None
    for row in cursor:
        uc=row[0]
    cursor.close()
    grey_db.close()

    if uc == None:
        return "User does not exist"

    try:
        user_action.write_points([{
                    "measurement":"user_action",
                    "tags":{
                            "id":toktok,
                            "action":"delete account"
                            },
                    "time":bf.timformat(),
                    "fields":{
                            "client-IP":IP_addr
                            }
                    }])

        grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
        cursor = grey_db.cursor(buffered=True)
        cursor.execute("delete from user where name=%s",(toktok,))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "User files and data have been completely deleted"
    except:
        return "INVALID, Server Error: Could not connect to database"



#################################
# CLUSTER ACTIONS
#################################

# Adds a new greyfish storage node to the cluster
@app.route("/grey/cluster/addme", methods=['POST'])
def cluster_addme():
    if not request.is_json:
        return "POST parameters could not be parsed"

    ppr = request.get_json()
    [error_occurs, missing_fields] = bf.error__l2_contains_l1(["orch_key", "MAX_STORAGE", "NODE_KEY"], ppr.keys())

    if error_occurs:
        return "INVALID: Lacking the following json fields to be read: "+missing_fields

    orch_key = ppr["orch_key"]
    MAX_STORAGE = int(ppr["MAX_STORAGE"]) # in KB
    NODE_KEY = ppr["NODE_KEY"]
    IP_addr = request.environ['REMOTE_ADDR']

    if not bf.valid_orchestra_key(orch_key):
        bf.cluster_action_log(IP_addr, IP_addr, "Attempted to attach storage node with invalid orchestra key", orch_key)
        return "INVALID key, cannot attach to cluster"

    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select * from node where ip=%s",(IP_addr,))
    uc=None
    for row in cursor:
        uc=row[0]
    cursor.close()
    grey_db.close()

    if uc != None:
        return "Node already attached"

    try:
        bf.cluster_action_log(IP_addr, IP_addr, "Attached new storage node", str(MAX_STORAGE)+" KB", NODE_KEY)
        grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
        cursor = grey_db.cursor(buffered=True)
        cursor.execute("insert into node(ip,total_space,free_space,node_key,status) values(%s,%s,%s,%s,'Available')",(IP_addr,MAX_STORAGE,MAX_STORAGE,NODE_KEY))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "New node attached correctly"
    except:
        return "INVALID, Server Error: Could not connect to database"



# Removes a storage node from the cluster
# Does not redistribute user data within the cluster
@app.route("/grey/cluster/removeme_as_is", methods=['POST'])
def removeme_as_is():
    if not request.is_json:
        return "POST parameters could not be parsed"

    ppr = request.get_json()
    [error_occurs, missing_fields] = bf.error__l2_contains_l1(["orch_key", "NODE_KEY", "node_IP"], ppr.keys())

    if error_occurs:
        return "INVALID: Lacking the following json fields to be read: "+missing_fields

    # Not necessary to be called from the node being disconnected
    orch_key = ppr["orch_key"]
    NODE_KEY = ppr["NODE_KEY"]
    node_IP = ppr["node_IP"]
    IP_addr = request.environ['REMOTE_ADDR']

    if not bf.valid_orchestra_key(orch_key):
        bf.cluster_action_log(IP_addr, IP_addr, "Attempted to remove storage node as is with invalid orchestra key", orch_key)
        return "INVALID key, cannot remove storage node"

    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select node_key from node where ip=%s",(node_IP,))
    nd=None
    for row in cursor:
        nd=row[0]
    cursor.close()
    grey_db.close()
    
    if nd == None:
        return "Node is not attached to cluster"

    # Checks the node key
    if nd != NODE_KEY:
        return "INVALID, incorrect node key"

    try:
        bf.cluster_action_log(IP_addr, node_IP, "Removed storage node as is")
        grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
        cursor = grey_db.cursor(buffered=True)
        cursor.execute("delete from node where ip=%s",(node_IP,))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "Node removed as is"
    except:
        return "INVALID, Server Error: Could not connect to database"

#################################
# FILE ACTIONS
#################################

# Uploads one file
# Directories must be separated by ++

@app.route("/grey/upload/<gkey>/<toktok>/<DIR>", methods=['POST'])
def file_upload(toktok, gkey, DIR=''):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "upload-file")
        return "INVALID key"

    # user must be added to the database beforehand
    if not bf.valid_user(toktok):
        bf.failed_login(gkey, IP_addr, toktok, "upload-file")
        return "INVALID user"

    file = request.files['file']
    fnam = file.filename

    # Avoids empty filenames and those with commas
    if fnam == '':
       return 'INVALID, no file uploaded'

    if ',' in fnam:
       return "INVALID, no ',' allowed in filenames"
   
    new_name = secure_filename(fnam)
    if not os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
    file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
    
    # find VM that can fit the file
    #request.files['file'].save('/tmp/foo')
    filesize = os.stat(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name)).st_size
    print("filesize: ",filesize, " bytes")
    vmip, nkey = bf.get_available_vm(filesize)
    print("VM: ",vmip," ,key: ",nkey)

    # upload the file to the first available VM
    files = {'file': open(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name), 'rb')}
    req = requests.post("http://"+vmip+":3443"+"/grey/storage_upload/"+nkey+"/"+toktok+"/"+DIR, files=files)

    # remove the file from local storage    
    if os.path.exists(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name)):
        os.remove(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
    
    if req.text == "INVALID node key":
        return 'File not uploaded due to invalid node key'

    action="allocate"
    bf.update_node_files(toktok,new_name,vmip,DIR,action)
    bf.update_node_space(vmip,nkey,filesize,action)
    
    return 'File succesfully uploaded to Greyfish'


# Deletes a file already present in the user
@app.route('/grey/delete_file/<gkey>/<toktok>/<FILE>/<DIR>')
def delete_file(toktok, gkey, FILE, DIR=''):
    
    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "delete-file")
        return "INVALID key"

    vmip,nkey = bf.get_file_vm(FILE,DIR)
    if vmip == None or nkey == None:
        return "Unable to locate the file"

    req = requests.get("http://"+vmip+":3443"+"/grey/storage_delete_file/"+nkey+"/"+toktok+"/"+FILE+"/"+DIR)

    if req.text=="INVALID node key" or req.text=="INVALID, User directory does not exist" or req.text == "File is not present in Greyfish":
        return req.text
    else:
        fsize=int(req.text)
        action="free"
        bf.update_node_files(toktok,FILE,vmip,DIR,action)
        bf.update_node_space(vmip,nkey,fsize,action)
        bf.greyfish_log(IP_addr, toktok, "delete", "single file", '/'.join(DIR.split('++')), str(FILE))
        return "File succesfully deleted from Greyfish storage"
 
 
# Returns a file
@app.route('/grey/grey/<gkey>/<toktok>/<FIL>/<DIR>')
def grey_file(gkey, toktok, FIL, DIR=''):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "download-file")
        return "INVALID key"
    
    vmip,nkey = bf.get_file_vm(FIL,DIR)
    if vmip == None or nkey == None:
        return "Unable to locate the file"
    
    req = requests.get("http://"+vmip+":3443"+"/grey/storage_grey/"+nkey+"/"+toktok+"/"+FIL+"/"+DIR)

    if "INVALID" in req.text:
        return req.text

    bf.greyfish_log(IP_addr, toktok, "download", "single file", '/'.join(DIR.split('++')), FIL)
    return req

#################################
# FOLDER ACTIONS
#################################

# Uploads one directory, it the directory already exists, then it deletes it and uploads the new contents
# Must be a tar file
@app.route("/grey/upload_dir/<gkey>/<toktok>/<DIR>", methods=['POST'])
def upload_dir(gkey, toktok, DIR):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "upload-dir")
        return "INVALID key"

    file = request.files['file']
    fnam = file.filename

    # Avoids empty filenames and those with commas
    if fnam == '':
        return 'INVALID, no file uploaded'

    if ',' in fnam:
        return "INVALID, no ',' allowed in filenames"

    # Untars the file, makes a directory if it does not exist
    if ('.tar.gz' not in fnam) and ('.tgz' not in fnam):
        return 'ERROR: Compression file not accepted, file must be .tgz or .tar.gz'

    new_name = secure_filename(fnam)
    vmip=nkey=dirsize=None

    try:
        if os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
            shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))

        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
        tar = tarfile.open(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name)
        tar.extractall(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        tar.close()
        dirsize = bf.get_dir_size(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name.split('.')[0])
        vmip, nkey = bf.get_available_vm(dirsize)

    except:
        return "Could not open tar file" 

    if vmip==None or nkey==None:
        return "Can't find the VM to fit the directory"
    
    files = {'file': open(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name), 'rb')}
    req = requests.post("http://"+vmip+":3443"+"/grey/storage_upload_dir/"+nkey+"/"+toktok+"/"+DIR, files=files)

    # remove the file from local storage    
    if os.path.exists(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name)):
        os.remove(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
        shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name.split('.')[0])


    if "INVALID" in req.text:
        return req.text
    dirsize=int(req.text)
    action="allocate"
    bf.update_node_folders(toktok,new_name,vmip,DIR,action)
    bf.update_node_space(vmip,nkey,dirsize,action)

    bf.greyfish_log(IP_addr, toktok, "upload", "dir", '/'.join(DIR.split('++')))
    return 'Directory succesfully uploaded to Greyfish'


# Deletes a directory
@app.route("/grey/delete_dir/<gkey>/<toktok>/<DIR>")
def delete_dir(toktok, gkey, DIR):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "delete-dir")
        return "INVALID key"

    vmip,nkey=bf.get_folder_vm(DIR)
    fsize=0
    for i in range(len(vmip)):
        req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_delete_dir/"+nkey[i]+"/"+toktok+"/"+DIR)
        if req.text=="INVALID node key" or req.text=="User directory does not exist":
            continue
        else:
            fsize=int(req.text)
            action="free"
            bf.update_node_folders(toktok,'',vmip[i],DIR,action)
            bf.update_node_space(vmip[i],nkey[i],fsize,action)
    bf.greyfish_log(IP_addr, toktok, "delete", "single dir", '/'.join(DIR.split('++')))
    return "Directory deleted"

# Downloads a directory
# Equivalent to downloading the tar file, since they are both equivalent
@app.route('/grey/grey_dir/<gkey>/<toktok>/<DIR>')
def grey_dir(gkey, toktok, DIR=''):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "download-dir")
        return "INVALID key"

    USER_DIR=GREYFISH_FOLDER+'DIR_'+str(toktok)+'/download/'
    if os.path.exists(USER_DIR):
        shutil.rmtree(USER_DIR)
    os.makedirs(USER_DIR)
    vmip,nkey=bf.get_folder_vm(DIR)
    for i in range(len(vmip)):
        req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_grey_dir/"+nkey[i]+"/"+toktok+"/"+DIR,stream=True)
        if "INVALID" in req.text:
            continue
        else:
            if os.path.exists(USER_DIR+'summary.tar.gz'):
                os.remove(USER_DIR+'summary.tar.gz')

            with open(USER_DIR+'summary.tar.gz','wb') as fd:
                for chunk in req.iter_content(chunk_size=128):
                    fd.write(chunk)

            with tarfile.open(USER_DIR+'summary.tar.gz',"r:gz") as tf:
                tf.extractall(USER_DIR)
            os.remove(USER_DIR+'summary.tar.gz')

    bf.greyfish_log(IP_addr, toktok, "download", "dir", '/'.join(DIR.split('++')))

    os.chdir(USER_DIR)
    tar = tarfile.open("summary.tar.gz", "w:gz")
    for ff in os.listdir('.'):
        tar.add(ff)
    tar.close()
    os.chdir(CURDIR)

    return send_file(USER_DIR+"summary.tar.gz")

if __name__ == '__main__':
   app.run()
