#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request
import os, shutil
import redis
from werkzeug.utils import secure_filename
#import base_functions as bf


app = Flask(__name__)
GREYFISH_FOLDER = "/greyfish/sandbox/"


URL_BASE = os.environ["URL_BASE"]
#REDIS_AUTH = os.environ["REDIS_AUTH"]



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



if __name__ == '__main__':
   app.run()
