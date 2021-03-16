#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file, jsonify
import os
import redis
import requests
import base_functions as bf
import mysql.connector as mysql_con
from werkzeug.utils import secure_filename
import tarfile, shutil, traceback
import checksums as ch
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
        cursor.execute("select ip from node")
        nodes=[]
        for row in cursor:
            nodes.append(row[0])
        for node in nodes:
            cursor.execute("insert into file set ip=%s, user_id=%s, id='', directory='', is_dir=TRUE",(node,toktok))
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
        cursor.execute("select distinct(ip) from file where user_id=%s",(toktok,))
        nodes=[]
        keys=None
        for row in cursor:
            nodes.append(row[0])
        for i in range(len(nodes)):
            cursor.execute("select node_key from node where ip=%s",(nodes[i],))
            for row in cursor:
                key=row[0]
            req = requests.get("http://"+nodes[i]+":3443"+"/grey/storage_delete_user/"+key+"/"+toktok)
            cursor.execute("delete from file where ip=%s and user_id=%s",(nodes[i],toktok))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "User files and data have been completely deleted"
    except:
        traceback.print_exc()
        return "INVALID, Server Error: Could not connect to database"



#################################
# CLUSTER ACTIONS
#################################

# returns ip_address of the instance
@app.route("/grey/cluster/whoami", methods=['GET'])
def whoami():
    IP_addr = request.environ['REMOTE_ADDR']
    return IP_addr
    
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
        cursor.execute("select name from user")
        users=[]
        for row in cursor:
            users.append(row[0])
        for user in users:
            cursor.execute("insert into file set ip=%s, user_id=%s, id='', directory='', is_dir=TRUE",(IP_addr,user))
        grey_db.commit()
        cursor.close()
        grey_db.close()
        return "New node attached correctly"
    except:
        traceback.print_exc()                     
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
        cursor.execute("delete from file where ip=%s",(node_IP,))
        grey_db.commit()
        cursor.close()
        grey_db.close()

        return "Node removed from the cluster"
    except:
        return "INVALID, Server Error: Could not connect to database"

#################################
# FILE ACTIONS
#################################

# Uploads one file
# Directories must be separated by ++

@app.route("/grey/upload/<gkey>/<toktok>", methods=['POST'], defaults={'DIR':''})
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

    dir_vm,_=bf.get_folder_vm(toktok,DIR)
    if not len(dir_vm):
        return "INVALID Directory does not exist"

    file = request.files['file']
    fnam = file.filename
    # Avoids empty filenames and those with commas
    if fnam == '':
       return 'INVALID, no file uploaded'
    if ',' in fnam:
       return "INVALID, no ',' allowed in filenames"

    #save file locally
    UPLOAD_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/upload'   
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)
    os.makedirs(UPLOAD_DIR)
    new_name = secure_filename(fnam)
    file.save(os.path.join(UPLOAD_DIR, new_name))
    
    # find VM that can fit the file
    filesize = os.stat(os.path.join(UPLOAD_DIR, new_name)).st_size
    vmip, nkey = bf.get_available_vms(filesize)
    if len(vmip)==0 or len(nkey)==0:
        os.remove(os.path.join(UPLOAD_DIR, new_name))
        return "Couldn't find any VM which can fit the file to be uploaded"

    ip=key=None
    for i in range(len(vmip)):
        if vmip[i] in dir_vm:
            ip=vmip[i]
            key=nkey[i]
            break 
        else:
            continue
    if ip==None or key==None:
        ip=vmip[0]
        key=nkey[0]


    # upload the file to the first available VM
    files = {'file': open(os.path.join(UPLOAD_DIR, new_name), 'rb')}
    if DIR=='':
        req = requests.post("http://"+ip+":3443"+"/grey/storage_upload/"+key+"/"+toktok, files=files)
    else:
        req = requests.post("http://"+ip+":3443"+"/grey/storage_upload/"+key+"/"+toktok+"/"+DIR, files=files)
    
    # remove the file from local storage    
    if os.path.exists(os.path.join(UPLOAD_DIR, new_name)):
        os.remove(os.path.join(UPLOAD_DIR, new_name))
   
    if "INVALID" in req.text:
        return req.text
    
    bf.greyfish_log(IP_addr, toktok, "upload", "single file", '/'.join(DIR.split('++')), new_name)
    return req.text


# Deletes a file already present in the user
@app.route('/grey/delete_file/<gkey>/<toktok>/<FILE>',defaults={'DIR':''})
@app.route('/grey/delete_file/<gkey>/<toktok>/<FILE>/<DIR>')
def delete_file(toktok, gkey, FILE, DIR=''):
    
    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "delete-file")
        return "INVALID key"

    vmip,nkey = bf.get_file_vm(toktok,FILE,DIR)
    if vmip == None or nkey == None:
        return "Unable to locate the file"

    if DIR=='':
        req = requests.get("http://"+vmip+":3443"+"/grey/storage_delete_file/"+nkey+"/"+toktok+"/"+FILE)
    else:
        req = requests.get("http://"+vmip+":3443"+"/grey/storage_delete_file/"+nkey+"/"+toktok+"/"+FILE+"/"+DIR)

    if "INVALID" in req.text or req.text == "File is not present in Greyfish":
        return req.text
                                                      
    bf.greyfish_log(IP_addr, toktok, "delete", "single file", '/'.join(DIR.split('++')), str(FILE))
    return req.text
    #return "File succesfully deleted from Greyfish storage"
 
# Returns a file
@app.route('/grey/grey/<gkey>/<toktok>/<FIL>',defaults={'DIR':''})
@app.route('/grey/grey/<gkey>/<toktok>/<FIL>/<DIR>')
def grey_file(gkey, toktok, FIL, DIR=''):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "download-file")
        return "INVALID key"
    
    vmip,nkey = bf.get_file_vm(toktok,FIL,DIR)
    if vmip == None or nkey == None:
        return "Unable to locate the file"
    
    if DIR=='':
        req = requests.get("http://"+vmip+":3443"+"/grey/storage_grey/"+nkey+"/"+toktok+"/"+FIL)
    else:
        req = requests.get("http://"+vmip+":3443"+"/grey/storage_grey/"+nkey+"/"+toktok+"/"+FIL+"/"+DIR)

    if "INVALID" in req.text:
        return req.text
    
    USER_DIR=GREYFISH_FOLDER+'DIR_'+str(toktok)+'/download/'
    if os.path.exists(USER_DIR):
        shutil.rmtree(USER_DIR)
    os.makedirs(USER_DIR)

    open(USER_DIR+FIL,'wb').write(req.content)

    os.chdir(USER_DIR)
    checksum = ch.sha256_checksum(FIL)
    checksumfile = open("checksum.txt","w")
    checksumfile.write(checksum)
    checksumfile.close()
    bf.update_file_checksum(toktok,FIL,vmip,DIR,checksum)
    
    if os.path.exists(USER_DIR+'summary.tar.gz'):
        os.remove(USER_DIR+'summary.tar.gz')
  
    tar = tarfile.open("summary.tar.gz", "w:gz")
    to_be_tarred = [x for x in os.listdir('.') if x != "summary.tar.gz"]
    for ff in to_be_tarred:
        tar.add(ff)
        os.remove(ff)
    tar.close()
    os.chdir(CURDIR)

    bf.greyfish_log(IP_addr, toktok, "download", "single file", '/'.join(DIR.split('++')), FIL)
    return send_file(USER_DIR+"summary.tar.gz")

#################################
# FOLDER ACTIONS
#################################

# Uploads one directory, it the directory already exists, then it deletes it and uploads the new contents
# Must be a tar file
@app.route("/grey/upload_dir/<gkey>/<toktok>/<DIR>", methods=['POST'])
def upload_dir(gkey, toktok, DIR=''):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "upload-dir")
        return "INVALID key"

    # user must be added to the database beforehand
    if not bf.valid_user(toktok):
        bf.failed_login(gkey, IP_addr, toktok, "upload-file")
        return "INVALID user"

    dir_vm,_=bf.get_folder_vm(toktok,'++'.join(DIR.split('++')[:-1]))
    if not len(dir_vm):
        return "INVALID Directory does not exist"

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
    
    #save file locally
    UPLOAD_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/upload'
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)
    os.makedirs(UPLOAD_DIR)
    new_name = secure_filename(fnam)
    vmip=nkey=dirsize=None

    try:
        file.save(os.path.join(UPLOAD_DIR, new_name))
        tar = tarfile.open(UPLOAD_DIR+'/'+new_name)
        tar.extractall(UPLOAD_DIR)
        tar.close()
        dirsize = bf.get_dir_size(UPLOAD_DIR+'/'+new_name.split('.')[0])
        vmip, nkey = bf.get_available_vms(dirsize)
    except:
        return "Could not open tar file" 

    if len(vmip)==0 or len(nkey)==0:
        os.remove(os.path.join(UPLOAD_DIR, new_name))
        shutil.rmtree(UPLOAD_DIR+'/'+new_name.split('.')[0])
        return "Can't find the VM to fit the directory"

    ip=key=None
    for i in range(len(vmip)):
        if vmip[i] in dir_vm:
            ip=vmip[i]
            key=nkey[i]
            break
        else:
            continue
    if ip==None or key==None:
        ip=vmip[0]
        key=nkey[0]
    
    files = {'file': open(os.path.join(UPLOAD_DIR, new_name), 'rb')}
    req = requests.post("http://"+ip+":3443"+"/grey/storage_upload_dir/"+key+"/"+toktok+"/"+DIR, files=files)

    # remove the file from local storage    
    if os.path.exists(os.path.join(UPLOAD_DIR, new_name)):
        os.remove(os.path.join(UPLOAD_DIR, new_name))
        shutil.rmtree(UPLOAD_DIR+'/'+new_name.split('.')[0])

    if "INVALID" in req.text or "ERROR" in req.text:
        return req.text

    bf.greyfish_log(IP_addr, toktok, "upload", "dir", '/'.join(DIR.split('++')))
    return req.text


# Deletes a directory
@app.route("/grey/delete_dir/<gkey>/<toktok>/<DIR>")
def delete_dir(toktok, gkey, DIR):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "delete-dir")
        return "INVALID key"

    vmip,nkey=bf.get_folder_vm(toktok,DIR)
    #fsize=0
    for i in range(len(vmip)):
        req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_delete_dir/"+nkey[i]+"/"+toktok+"/"+DIR)
        if req.text=="INVALID node key" or req.text=="User directory does not exist":
            continue
    bf.greyfish_log(IP_addr, toktok, "delete", "single dir", '/'.join(DIR.split('++')))
    return req.text

# Downloads a directory
# Equivalent to downloading the tar file, since they are both equivalent
@app.route('/grey/grey_dir/<gkey>/<toktok>',defaults={'DIR':''})
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

    vmip,nkey=bf.get_folder_vm(toktok,DIR)
    for i in range(len(vmip)):
        if DIR=='':
            req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_grey_dir/"+nkey[i]+"/"+toktok,stream=True)
            delete = requests.get("http://"+vmip[i]+":3443/grey/storage_delete_file/"+nkey[i]+"/"+toktok+"/summary.tar.gz")
        else:
            req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_grey_dir/"+nkey[i]+"/"+toktok+"/"+DIR,stream=True)
            delete = requests.get("http://"+vmip[i]+":3443/grey/storage_delete_file/"+nkey[i]+"/"+toktok+"/summary.tar.gz/"+DIR)
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


    checksum = ch.sha256_checksum_dir(USER_DIR)
    bf.update_folder_checksum(toktok,vmip,DIR,checksum)
    os.chdir(USER_DIR)
    checksumfile = open("checksum.txt","w")
    checksumfile.write(checksum)
    checksumfile.close()

    tar = tarfile.open("summary.tar.gz", "w:gz")
    to_be_tarred = [x for x in os.listdir('.') if x != "summary.tar.gz"]
    for ff in to_be_tarred:
        tar.add(ff)
        if os.path.isdir(ff):
            shutil.rmtree(ff)
        else:
            os.remove(ff)
    tar.close()
    os.chdir(CURDIR)
    
    bf.greyfish_log(IP_addr, toktok, "download", "dir", '/'.join(DIR.split('++')))
    return send_file(USER_DIR+"summary.tar.gz")

@app.route('/grey/grey_dir_json/<gkey>/<toktok>')
def grey_dir_json(gkey, toktok):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "download-dir")
        return "INVALID key"

    USER_DIR=GREYFISH_FOLDER+'DIR_'+str(toktok)+'/download'
    if os.path.exists(USER_DIR):
        shutil.rmtree(USER_DIR)
    os.makedirs(USER_DIR)

    vmip,nkey=bf.get_folder_vm(toktok,'')
    for i in range(len(vmip)):
        req = requests.get("http://"+vmip[i]+":3443"+"/grey/storage_grey_dir/"+nkey[i]+"/"+toktok,stream=True)
        delete = requests.get("http://"+vmip[i]+":3443/grey/storage_delete_file/"+nkey[i]+"/"+toktok+"/summary.tar.gz")
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

    json = jsonify(bf.structure_in_json(USER_DIR))
    os.chdir(USER_DIR)
    for ff in os.listdir('.'):
        if os.path.isdir(ff):
            shutil.rmtree(ff)
        else:
            os.remove(ff)

    return json
if __name__ == '__main__':
   app.run()
