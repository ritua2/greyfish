#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request, send_file
import os, shutil
import redis
from werkzeug.utils import secure_filename
import base_functions as bf
import tarfile

app = Flask(__name__)
GREYFISH_FOLDER = "/greyfish/sandbox/"


URL_BASE = os.environ["URL_BASE"]
#REDIS_AUTH = os.environ["REDIS_AUTH"]
CURDIR = dir_path = os.path.dirname(os.path.realpath(__file__))


#################################
# FILE ACTIONS
#################################

# Uploads one file
# Directories must be separated by ++

@app.route("/grey/storage_upload/<nkey>/<toktok>/<DIR>", methods=['POST'])
def result_upload(nkey,toktok,DIR=''):
    
    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok))

    file = request.files['file']
    fnam = file.filename
    #print("Storage node data: ")
    #print("User: ",toktok)
    #print("nkey: ",nkey)
    #print("DIR: ",DIR)

    # Ensures no commands within the filename
    new_name = secure_filename(fnam)

    if not os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
    file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
    return 'File succesfully uploaded to Greyfish'

# Deletes a file already present in the user
@app.route('/grey/storage_delete_file/<nkey>/<toktok>/<FILE>/<DIR>')
def delete_file(toktok, nkey, FILE, DIR=''):

    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
       return 'INVALID, User directory does not exist'

    try:
        file_stats=os.stat(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+str(FILE))
        file_size=file_stats.st_size
        os.remove(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+str(FILE))
        #bf.greyfish_log(IP_addr, toktok, "delete", "single file", '/'.join(DIR.split('++')), str(FILE))
        print('File succesfully deleted from Greyfish storage')
        return str(file_size)

    except:
        return 'File is not present in Greyfish'



# Returns a file
@app.route('/grey/storage_grey/<nkey>/<toktok>/<FIL>/<DIR>')
def grey_file(nkey, toktok, FIL, DIR=''):

    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
       return 'INVALID, User directory does not exist'

    USER_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'
    if str(FIL) not in os.listdir(USER_DIR):
       return 'INVALID, File not available'

    return send_file(USER_DIR+str(FIL), as_attachment=True)

#################################
# FOLDER ACTIONS
#################################

# Uploads one directory, it the directory already exists, then it deletes it and uploads the new contents
# Must be a tar file
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

    try:
        if os.path.exists(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))):
            shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))

        os.makedirs(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        file.save(os.path.join(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')), new_name))
        tar = tarfile.open(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name)
        tar.extractall(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++')))
        tar.close()
        os.remove(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'+new_name)
        dirsize = bf.get_dir_size(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/')
        print('Directory succesfully uploaded to Greyfish')
        return str(dirsize)
    except:
        return "Could not open tar file" 

    #return 'Directory succesfully uploaded to Greyfish'

# Deletes a directory
@app.route("/grey/storage_delete_dir/<nkey>/<toktok>/<DIR>")
def delete_dir(toktok, nkey, DIR):

    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    dirsize=0
    try:
        dirsize = bf.get_dir_size(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/')
        shutil.rmtree(GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/')
        print("Directory deleted")
        return str(dirsize)

    except:
        return "User directory does not exist"

# Downloads a directory
# Equivalent to downloading the tar file, since they are both equivalent
@app.route('/grey/storage_grey_dir/<nkey>/<toktok>/<DIR>')
def grey_dir(nkey, toktok, DIR=''):

    if not nkey == os.environ['NODE_KEY']:
        return "INVALID node key"

    if str('DIR_'+toktok) not in os.listdir(GREYFISH_FOLDER):
        return 'INVALID, User directory does not exist'

    USER_DIR = GREYFISH_FOLDER+'DIR_'+str(toktok)+'/'+'/'.join(DIR.split('++'))+'/'

    if not os.path.exists(USER_DIR):
        return 'INVALID, Directory not available'

    os.chdir(USER_DIR)

    tar = tarfile.open("summary.tar.gz", "w:gz")
    for ff in os.listdir('.'):
        tar.add(ff)
    tar.close()

    os.chdir(CURDIR)

    return send_file(USER_DIR+"summary.tar.gz")

if __name__ == '__main__':
   app.run()
