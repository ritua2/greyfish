#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file, jsonify
import os, requests, tarfile, shutil
import base_functions as bf
import mysql.connector as mysql_con
import checksums as ch
app = Flask(__name__)

GREYFISH_FOLDER = "/greyfish/sandbox/"
CURDIR = dir_path = os.path.dirname(os.path.realpath(__file__))

#################################
# FILE ACTIONS
#################################
 
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
        return "INVALID, Unable to locate the file"
    
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
    if len(vmip)==0:
        return "INVALID, Unable to locate the directory"
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

#################################
# VIEW ACTIONS
#################################

# Returns directory structure for a user in json format
@app.route('/grey/grey_dir_json/<gkey>/<toktok>')
def grey_dir_json(gkey, toktok):

    IP_addr = request.environ['REMOTE_ADDR']
    if not bf.valid_key(gkey, toktok):
        bf.failed_login(gkey, IP_addr, toktok, "download-dir")
        return "INVALID key"

    USER_DIR=GREYFISH_FOLDER+'DIR_'+str(toktok)+'/home'
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
