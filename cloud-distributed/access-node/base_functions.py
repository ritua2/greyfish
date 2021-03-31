"""
BASICS

Contains a set of functions that are called accross the other APIs
"""

from pathlib import Path
from influxdb import InfluxDBClient
import os, datetime, time
import mysql.connector as mysql_con
import checksums as ch


# Checks if the provided user key is valid
def valid_key(ukey, username):
    if ukey == os.environ['greyfish_key']:
        return True

    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select username from greykeys where token=%s",(ukey,))
    user=None
    for row in cursor:
        user=row[0]

    if user == None:
        cursor.close()
        grey_db.close()
        return False

    if user == username:
        cursor.execute("delete from greykeys where token=%s",(ukey,))
        grey_db.commit()
        cursor.close()
        grey_db.close()
        return True

    cursor.close()
    grey_db.close()
    return False



# Checks if the orchestra key is valid
def valid_orchestra_key(provided_key):
    if provided_key == os.environ["orchestra_key"]:
        return True
    else:
        return False

# Checks if the user is valid
def valid_user(unam):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select * from user where name=%s",(unam,))
    uc=None
    for row in cursor:
        uc=row[0]

    cursor.close()
    grey_db.close()
    if uc != None:
        return True
    else:
        return False

# Update file checksum
def update_file_checksum(toktok,new_name,vmip,DIR,checksum):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("update file set checksum=%s where id=%s and directory=%s and user_id=%s and ip=%s",(checksum,new_name,DIR,toktok,vmip))
    grey_db.commit()
    cursor.close()
    grey_db.close()

# Update folder checksum
def update_folder_checksum(toktok,vmip,DIR,checksum):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("update file set checksum=%s where id='' and directory=%s and user_id=%s",(checksum,DIR,toktok))
    grey_db.commit()
    cursor.close()
    grey_db.close()
 
# get VM containing the file 
def get_file_vm(userid,file,dir):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select ip from file where user_id=%s and id=%s and directory=%s",(userid,file,dir))
    ip=None
    nkey=None
    for row in cursor:
        ip=row[0]
    if ip is not None:
        cursor.execute("select node_key from node where ip=%s",(ip,))
        for row in cursor:
            nkey=row[0]
    cursor.close()
    grey_db.close()
    return ip,nkey

# get VMs containing the folder 
def get_folder_vm(userid,dir):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select ip from file where user_id=%s and directory=%s and is_dir=TRUE",(userid,dir))
    ip=[]
    nkey=[]
    for row in cursor:
        ip.append(row[0])
    if len(ip)>0:
        for vmip in ip:
            cursor.execute("select node_key from node where ip=%s",(vmip,))
            for row in cursor:
                nkey.append(row[0])
    cursor.close()
    grey_db.close()
    return ip,nkey

# Creates a new key (new dir) in the dictionary
# fpl (arr) (str): Contains the list of subsequent directories
# exdic (dict)
def create_new_dirtag(fpl, exdic):
    # New working dictionary
    nwd = exdic

    for qq in range(0, len(fpl)-1):
        nwd = nwd[fpl[qq]]

    # Adds one at the end
    nwd[fpl[-1]] = {"files":[]}

    return exdic


# Returns a dictionary showing all the files in a directory (defaults to working directory)
def structure_in_json(PATH = '.'):
    FSJ = {PATH.split('/')[-1]:{"files":[]}}

    # Includes the current directory
    # Replaces everything before the user
    unpart = '/'.join(PATH.split('/')[:-1])+'/'

    for ff in [str(x).replace(unpart, '').split('/') for x in Path(PATH).glob('**/*')]:

        if os.path.isdir(unpart+'/'.join(ff)):
            create_new_dirtag(ff, FSJ)
            continue

        # Files get added to the list, files
        # Loops through the dict
        nwd = FSJ
        for hh in range(0, len(ff)-1):
            nwd = nwd[ff[hh]]

        nwd["files"].append(ff[-1])

    return FSJ


# Returns a dictionary showing all the files in a directory (defaults to working directory)
def structure_in_json_user(userid):
    files={}
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select id, directory from file where user_id=%s group by id, directory order by directory",(userid,))
    file=[]
    dir=[]
    for row in cursor:
        if row[0]=='' and row[1]=='':
            continue
        file.append(row[0])
        dir.append(row[1])
    for x,y in zip(file,dir):
        files.setdefault(y.replace('++','/'),[]).append(x)
    for k in files:
        if len(files[k])>1 and '' in files[k]:
            files[k].remove('')
    cursor.close()
    grey_db.close()
    return files

# Returns a string in UTC time in the format YYYY-MM-DD HH:MM:SS.XXXXXX (where XXXXXX are microseconds)
def timformat():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")


# Logs a failed login
# logkey (str): Key used
# IP (str): IP used
# unam (str): username used
# action (str)
# due_to (str): Reason for failed login, most likely due to incorrect key

def failed_login(logkey, IP, unam, action, due_to="incorrect_key"):
    FC = InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_WRITE_USER'], 
        password = os.environ['INFLUXDB_WRITE_USER_PASSWORD'], database = 'failed_login')

    # Finds if the user is valid or not
    if valid_user(unam):
        valid_usr="1"
    else:
        valid_usr="0"

    FC.write_points([{
                    "measurement":"bad_credentials",
                    "tags":{
                            "id":unam,
                            "valid_account":valid_usr,
                            "action":action,
                            "reason":due_to
                            },
                    "time":timformat(),
                    "fields":{
                            "client-IP":IP,
                            "logkey":logkey
                            }
                    }])



# Generic greyfish action
# action_id (str): ID of the pertaining action
# specs (str): Specific action detail
def greyfish_log(IP, unam, action, spec1=None, spec2=None, spec3=None):
    glog = InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_WRITE_USER'], 
        password = os.environ['INFLUXDB_WRITE_USER_PASSWORD'], database = 'greyfish')

    glog.write_points([{
                    "measurement":"action_logs",
                    "tags":{
                            "id":unam,
                            "action":action,
                            "S1":spec1
                            },
                    "time":timformat(),
                    "fields":{
                            "client-IP":IP,
                            "S2":spec2,
                            "S3":spec3
                            }
                    }])