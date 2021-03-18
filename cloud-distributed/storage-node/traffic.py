#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file
from werkzeug.utils import secure_filename
import os, shutil, requests, tarfile, traceback
import base_functions as bf

app = Flask(__name__)
GREYFISH_FOLDER = "/greyfish/sandbox/"
URL_BASE = os.environ["URL_BASE"]
CURDIR = dir_path = os.path.dirname(os.path.realpath(__file__))

#################################
# FILE ACTIONS
#################################

# Uploads one file
# Directories must be separated by ++
@app.route("/grey/storage_upload/<nkey>/<toktok>", methods=['POST'], defaults={'DIR':''})
@app.route("/grey/storage_upload/<nkey>/<toktok>/<DIR>", methods=['POST'])
def result_upload(nkey,toktok,DIR=''):    
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok))

    file = request.files['file']
    fnam = file.filename

    # Ensures no commands within the filename
    new_name = secure_filename(fnam)

    if not os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
    file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
    res = requests.get("http://"+URL_BASE+":2443/grey/cluster/whoami")
    ip = res.text
    bf.add_file(ip,toktok,DIR,new_name)
    bf.update_node_space(ip)
    return 'File succesfully uploaded to Greyfish'

# Deletes a file already present in the user
@app.route('/grey/storage_delete_file/<nkey>/<toktok>/<FILE>', defaults={'DIR':''})
@app.route('/grey/storage_delete_file/<nkey>/<toktok>/<FILE>/<DIR>')
def delete_file(toktok, nkey, FILE, DIR=''):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
       return 'INVALID, User directory does not exist'

    try:
        res = requests.get("http://"+URL_BASE+":2443/grey/cluster/whoami")
        ip = res.text
        bf.remove_file(ip,toktok,DIR,FILE)
        os.remove(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+str(FILE))
        bf.update_node_space(ip)
        return 'File succesfully deleted from Greyfish storage'
    except:
        return 'INVALID, File is not present in Greyfish'

# Returns a file
@app.route("/grey/storage_grey/<nkey>/<toktok>/<FIL>", defaults={'DIR':''})
@app.route('/grey/storage_grey/<nkey>/<toktok>/<FIL>/<DIR>')
def grey_file(nkey, toktok, FIL, DIR=''):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
       return 'INVALID, User directory does not exist'

    USER_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))
    if str(FIL) not in os.listdir(USER_DIR):
       return 'INVALID, File not available'

    return send_file(os.path.join(USER_DIR, str(FIL)), as_attachment=True)

#################################
# FOLDER ACTIONS
#################################

# Uploads one directory, it the directory already exists, then it deletes it and uploads the new contents
# Must be a tar file
@app.route("/grey/storage_upload_dir/<nkey>/<toktok>", defaults={'DIR':''}, methods=['POST'])
@app.route("/grey/storage_upload_dir/<nkey>/<toktok>/<DIR>", methods=['POST'])
def upload_dir(nkey, toktok, DIR):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok))

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
    res = requests.get("http://"+URL_BASE+":2443/grey/cluster/whoami")
    ip = res.text

    try:
        if not os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
            os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
        tar = tarfile.open(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name)
        tar.extractall(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        tar.close()
        os.remove(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name)
    except:
        traceback.print_exc()
        return "ERROR: Could not open tar file" 

    try:
        for root, dirs, files in os.walk(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'):
            bf.add_dir(ip,toktok,'++'.join(root.replace(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/','').split('/')))
            for file in files:
                bf.add_file(ip,toktok,'++'.join(root.replace(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/','').split('/')),file)
    except:
        traceback.print_exc()
        return "ERROR: can't update database"
    bf.update_node_space(ip)
    return 'Directory succesfully uploaded to Greyfish'

# Deletes a directory
@app.route("/grey/storage_delete_dir/<nkey>/<toktok>/<DIR>")
def delete_dir(toktok, nkey, DIR):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID, node key"

    try:        
        res = requests.get("http://"+URL_BASE+":2443/grey/cluster/whoami")
        ip = res.text
        bf.remove_dir(ip,toktok,DIR)
        shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/')
        bf.update_node_space(ip)
        return "Directory deleted"
    except:
        return "INVALID, User directory does not exist"

# Downloads a directory
# Equivalent to downloading the tar file, since they are both equivalent
@app.route('/grey/storage_grey_dir/<nkey>/<toktok>',defaults={'DIR':''})
@app.route('/grey/storage_grey_dir/<nkey>/<toktok>/<DIR>')
def grey_dir(nkey, toktok, DIR=''):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
        return 'INVALID, User directory does not exist'

    USER_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))

    if not os.path.exists(USER_DIR):
        return 'INVALID, Directory not available'

    os.chdir(USER_DIR)

    tar = tarfile.open("summary.tar.gz", "w:gz")
    to_be_tarred = [x for x in os.listdir('.') if x != "summary.tar.gz"]

    for ff in to_be_tarred:
        tar.add(ff)
    tar.close()

    os.chdir(CURDIR)
    return send_file(os.path.join(USER_DIR,"summary.tar.gz"), as_attachment=True)

# Deletes a user directory
@app.route("/grey/storage_delete_user/<nkey>/<toktok>")
def delete_user_dir(nkey, toktok):
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID, node key"

    try:
        res = requests.get("http://"+URL_BASE+":2443/grey/cluster/whoami")
        ip = res.text
        shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok))
        bf.update_node_space(ip)
        return "User directory deleted"
    except:
        return "INVALID, User directory does not exist"
if __name__ == '__main__':
   app.run()
