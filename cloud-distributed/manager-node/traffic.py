#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file, jsonify
import os, requests, tarfile, shutil, traceback
import base_functions as bf
import mysql.connector as mysql_con
from werkzeug.utils import secure_filename
app = Flask(__name__)

URL_BASE = os.environ["URL_BASE"]
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
            req = requests.get("https://"+nodes[i]+":3443"+"/grey/storage_delete_user/"+key+"/"+toktok, verify=False)
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
    #if not len(dir_vm):
     #   return "INVALID Directory does not exist"

    file = request.files['file']
    fnam = file.filename
    # Avoids empty filenames and those with commas
    if fnam == '':
       return 'INVALID, no file uploaded'
    if ',' in fnam:
       return "INVALID, no ',' allowed in filenames"

    new_name = secure_filename(fnam)
    filevm,vmkey=bf.get_file_vm(toktok,new_name,DIR)
    if filevm != None and vmkey != None:
        if DIR=='':
            delete = requests.get("https://"+filevm+":3443/grey/storage_delete_file/"+vmkey+"/"+toktok+"/"+new_name, verify=False)
        else:
            delete = requests.get("https://"+filevm+":3443/grey/storage_delete_file/"+vmkey+"/"+toktok+"/"+new_name+"/"+DIR, verify=False)


    #save file locally
    UPLOAD_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/upload'   
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)
    os.makedirs(UPLOAD_DIR)
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
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload/"+key+"/"+toktok, files=files, verify=False)
    else:
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload/"+key+"/"+toktok+"/"+DIR, files=files, verify=False)
    
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
        req = requests.get("https://"+vmip+":3443"+"/grey/storage_delete_file/"+nkey+"/"+toktok+"/"+FILE, verify=False)
    else:
        req = requests.get("https://"+vmip+":3443"+"/grey/storage_delete_file/"+nkey+"/"+toktok+"/"+FILE+"/"+DIR, verify=False)

    if "INVALID" in req.text or req.text == "File is not present in Greyfish":
        return req.text
                                                      
    bf.greyfish_log(IP_addr, toktok, "delete", "single file", '/'.join(DIR.split('++')), str(FILE))
    return req.text

#################################
# FOLDER ACTIONS
#################################

# Uploads one directory, if the directory already exist then exits the program else uploads the contents.
# Must be a tar file
@app.route("/grey/upload_dir/<gkey>/<toktok>", defaults={'DIR':''},methods=['POST'])
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
    
    parent_dir_vm,_=bf.get_folder_vm(toktok,DIR)
    #parent_dir_vm,_=bf.get_folder_vm(toktok,'++'.join(DIR.split('++')[:-1]))
    #if not len(parent_dir_vm):
    #    return "INVALID Directory does not exist"

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
        dirnames=[x for x in os.listdir(UPLOAD_DIR) if x != new_name]
        if len(dirnames) != 1 or not os.path.isdir(os.path.join(UPLOAD_DIR,dirnames[0])):
            shutil.rmtree(UPLOAD_DIR)
            return "Only one directory can be uploaded at a time"

        if DIR=='':
            dir_vm,_=bf.get_folder_vm(toktok,dirnames[0])
        else:
            dir_vm,_=bf.get_folder_vm(toktok,DIR+'++'+dirnames[0])
        if len(dir_vm)>0:
            shutil.rmtree(UPLOAD_DIR)
            return "Directory exists. Try deleting or replacing the directory"

        dirsize = bf.get_dir_size(os.path.join(UPLOAD_DIR,dirnames[0]))
        vmip, nkey = bf.get_available_vms(dirsize)
    except:
        traceback.print_exc()
        shutil.rmtree(UPLOAD_DIR)
        return "Could not open tar file" 

    if len(vmip)==0 or len(nkey)==0:
        shutil.rmtree(UPLOAD_DIR)
        return "Can't find the VM to fit the directory"

    ip=key=None
    for i in range(len(vmip)):
        if vmip[i] in parent_dir_vm:
            ip=vmip[i]
            key=nkey[i]
            break
        else:
            continue
    if ip==None or key==None:
        ip=vmip[0]
        key=nkey[0]
    
    files = {'file': open(os.path.join(UPLOAD_DIR, new_name), 'rb')}
    if DIR=='':
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload_dir/"+key+"/"+toktok, files=files, verify=False)
    else:
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload_dir/"+key+"/"+toktok+"/"+DIR, files=files, verify=False)

    # remove the file from local storage    
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)

    if "INVALID" in req.text or "ERROR" in req.text:
        return req.text

    #bf.greyfish_log(IP_addr, toktok, "upload", "dir", '/'.join(DIR.split('++')))
    return req.text

# Uploads one directory, it the directory already exists, then it deletes it and uploads the new contents
# Must be a tar file
@app.route("/grey/upload_replace_dir/<gkey>/<toktok>", defaults={'DIR':''},methods=['POST'])
@app.route("/grey/upload_replace_dir/<gkey>/<toktok>/<DIR>", methods=['POST'])
def upload_replace_dir(gkey, toktok, DIR=''):
    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        #bf.failed_login(gkey, IP_addr, toktok, "upload-dir")
        return "INVALID key"

    # user must be added to the database beforehand
    if not bf.valid_user(toktok):
        #bf.failed_login(gkey, IP_addr, toktok, "upload-file")
        return "INVALID user"
    
    parent_dir_vm,_=bf.get_folder_vm(toktok,DIR)
    #parent_dir_vm,_=bf.get_folder_vm(toktok,'++'.join(DIR.split('++')[:-1]))
    #if not len(parent_dir_vm):
    #    return "INVALID Directory does not exist"

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
        dirnames=[x for x in os.listdir(UPLOAD_DIR) if x != new_name]
        if len(dirnames) != 1 or not os.path.isdir(os.path.join(UPLOAD_DIR,dirnames[0])):
            shutil.rmtree(UPLOAD_DIR)
            return "Only one directory can be uploaded at a time"

        if DIR=='':
            dir_vm,_=bf.get_folder_vm(toktok,dirnames[0])
        else:
            dir_vm,_=bf.get_folder_vm(toktok,DIR+'++'+dirnames[0])
        if len(dir_vm)>0:
            if DIR=='':
                delete = requests.get("https://"+URL_BASE+":2443/grey/delete_dir/"+gkey+"/"+toktok+"/"+dirnames[0], verify=False)
            else:
                delete = requests.get("https://"+URL_BASE+":2443/grey/delete_dir/"+gkey+"/"+toktok+"/"+DIR+'++'+dirnames[0], verify=False)
        dirsize = bf.get_dir_size(os.path.join(UPLOAD_DIR,dirnames[0]))
        vmip, nkey = bf.get_available_vms(dirsize)
    except:
        traceback.print_exc()
        shutil.rmtree(UPLOAD_DIR)
        return "Could not open tar file" 

    if len(vmip)==0 or len(nkey)==0:
        shutil.rmtree(UPLOAD_DIR)
        return "Can't find the VM to fit the directory"

    ip=key=None
    for i in range(len(vmip)):
        if vmip[i] in parent_dir_vm:
            ip=vmip[i]
            key=nkey[i]
            break
        else:
            continue
    if ip==None or key==None:
        ip=vmip[0]
        key=nkey[0]
    
    files = {'file': open(os.path.join(UPLOAD_DIR, new_name), 'rb')}
    if DIR=='':
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload_dir/"+key+"/"+toktok, files=files, verify=False)
    else:
        req = requests.post("https://"+ip+":3443"+"/grey/storage_upload_dir/"+key+"/"+toktok+"/"+DIR, files=files, verify=False)

    # remove the file from local storage    
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)

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
    if len(vmip)==0 or len(nkey)==0:
        return "INVALID, Directory not found"
    #fsize=0
    for i in range(len(vmip)):
        req = requests.get("https://"+vmip[i]+":3443"+"/grey/storage_delete_dir/"+nkey[i]+"/"+toktok+"/"+DIR, verify=False)
        if req.text=="INVALID node key" or req.text=="User directory does not exist":
            continue
    bf.greyfish_log(IP_addr, toktok, "delete", "single dir", '/'.join(DIR.split('++')))
    return req.text

if __name__ == '__main__':
   app.run()
