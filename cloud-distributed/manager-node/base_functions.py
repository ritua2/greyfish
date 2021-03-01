"""
BASICS

Contains a set of functions that are called accross the other APIs
"""

import os
import datetime, time
from pathlib import Path
from influxdb import InfluxDBClient
import redis
import mysql.connector as mysql_con



# Returns the list of elements of l1 not in l2
# l1 (arr) (generic)
# l2 (arr) (generic)
# returns (arr)
def l2_contains_l1(l1, l2):
    return[elem for elem in l1 if elem not in l2]



# separator (str)
def error__l2_contains_l1(l1, l2, separator=","):

    check = l2_contains_l1(l1, l2)

    if check:
        return [True, separator.join([str(a) for a in check])]
    else:
        return [False, ""]


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

# Get available VM with required space
def get_available_vm(size):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select ip, node_key from node where status='Available' and free_space > %s order by free_space desc",(size,))
    ip=None
    nkey=None
    for row in cursor:
        ip=row[0]
        nkey=row[1]

    cursor.close()
    grey_db.close()
    return ip,nkey
   
# add each directory with the parents
def add_dirs(DIR,uploaddir,vmip,toktok,is_file):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    dirs=DIR.split('++')
    #print(dirs)
    for i in range(len(dirs)):
        plus="++"
        user=None
        cursor.execute("select user_id from file where ip=%s and id=%s and directory=%s",(vmip,dirs[i],plus.join(dirs[:i])))
        for row in cursor:
            user=row[0]
        if user == None:
            cursor.execute("insert into file(id,user_id,ip,directory,is_dir) values(%s,%s,%s,%s,TRUE)",(dirs[i],toktok,vmip,plus.join(dirs[:i])))
            grey_db.commit()
        else:
            continue
        #print('Add: ',dirs[i]," and: ",plus.join(dirs[:i]))
    user=None
    if is_file:
        cursor.execute("select user_id from file where ip=%s and id=%s and directory=%s",(vmip,uploaddir,DIR))
        for row in cursor:
            user=row[0]
        if user == None:
            cursor.execute("insert into file(id,user_id,ip,directory) values(%s,%s,%s,%s)",(uploaddir,toktok,vmip,DIR))
            grey_db.commit()
    else:
        cursor.execute("select user_id from file where ip=%s and id=%s and directory=%s",(vmip,uploaddir.split('.')[0],DIR))
        for row in cursor:
            user=row[0]
        if user == None:
            cursor.execute("insert into file(id,user_id,ip,directory,is_dir) values(%s,%s,%s,%s,TRUE)",(uploaddir.split('.')[0],toktok,vmip,DIR))
            grey_db.commit()
    cursor.close()
    grey_db.close()
    #print('Add: ',uploaddir.split('.')[0]," and: ",DIR)

# remove each directory and sub-directories
def delete_dirs(directory,vmip,toktok):
    dir=directory.split('++')[-1]
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("delete from file where id=%s and ip=%s and user_id=%s",(dir,vmip,toktok))
    cursor.execute("delete from file where directory like %s and ip=%s and user_id=%s",("%"+dir+"%",vmip,toktok))
    grey_db.commit()
    cursor.close()
    grey_db.close()


# Log the location of the file for the user
def update_node_files(toktok,new_name,vmip,DIR,action):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    if action == "allocate":
        add_dirs(DIR,new_name,vmip,toktok,True)
    if action == "free":
        cursor.execute("delete from file where id=%s and user_id=%s and ip=%s",(new_name,toktok,vmip))
    grey_db.commit()
    cursor.close()
    grey_db.close()

# Log the location of the folders for the user
def update_node_folders(toktok,new_name,vmip,DIR,action):
    if action == "allocate":
        add_dirs(DIR,new_name,vmip,toktok)
    if action == "free":
        delete_dirs(DIR,vmip,toktok)
   
# Update available space on the VM
def update_node_space(vmip,nkey,filesize,action):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select free_space from node where ip=%s",(vmip,))
    available_space=None
    for row in cursor:
        available_space=int(row[0])

    print("Available size",available_space)

    if action == "allocate":
        available_space = available_space-filesize    
    if action == "free":
        available_space = available_space+filesize
    
    print("Available size",available_space)

    if available_space > 10:
        cursor.execute("update node set free_space=%s,status='Available' where ip=%s and node_key=%s",(available_space,vmip,nkey))
    else:
        cursor.execute("update node set free_space=%s,status='Full' where ip=%s and node_key=%s",(available_space,vmip,nkey))
   
    grey_db.commit()
    cursor.close()
    grey_db.close()
 
# get VM containing the file 
def get_file_vm(file,dir):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select ip from file where id=%s and directory=%s",(file,dir))
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
def get_folder_vm(dir):
    grey_db = mysql_con.connect(host = os.environ["URL_BASE"] , port = 6603, user = os.environ["MYSQL_USER"] , password = os.environ["MYSQL_PASSWORD"], database = os.environ["MYSQL_DATABASE"])
    cursor = grey_db.cursor(buffered=True)
    cursor.execute("select ip from file where id=%s and directory=%s",(dir.split('++')[-1],'++'.join(dir.split('++')[:-1])))
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

# return size of the directory in bytes
def get_dir_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size



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



# Given two lists, returns those values that are lacking in the second
# Empty if list 2 contains those elements
def l2_contains_l1(l1, l2):
    return[elem for elem in l1 if elem not in l2]



# Returns a administrative client 
# Default refers to the basic grey server
def idb_admin(db='greyfish'):

    return InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_ADMIN_USER'], 
        password = os.environ['INFLUXDB_ADMIN_PASSWORD'], database = db)


# Returns an incfluxdb client with read-only access
def idb_reader(db='greyfish'):

    return InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_READ_USER'], 
        password = os.environ['INFLUXDB_READ_USER_PASSWORD'], database = db)


# Returns an incfluxdb client with write privileges
def idb_writer(db='greyfish'):

    return InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_WRITE_USER'], 
        password = os.environ['INFLUXDB_WRITE_USER_PASSWORD'], database = db)


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



# Saves a cluster action such as adding or removing a node
def cluster_action_log(IP, unam, action, spec1=None, spec2=None, spec3=None):

    cl_log = InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_WRITE_USER'], 
        password = os.environ['INFLUXDB_WRITE_USER_PASSWORD'], database = 'cluster_actions')

    cl_log.write_points([{
                    "measurement":"cluster",
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


# Admin greyfish action
# self_identifier (str): Who the user identifies as while executing the action
# action_id (str): ID of the pertaining action
# specs (str): Specific action detail
def greyfish_admin_log(IP, self_identifier, action, spec1=None, spec2=None, spec3=None):

    glog = InfluxDBClient(host = os.environ['URL_BASE'], port = 8086, username = os.environ['INFLUXDB_WRITE_USER'], 
        password = os.environ['INFLUXDB_WRITE_USER_PASSWORD'], database = 'greyfish')

    glog.write_points([{
                    "measurement":"admin_logs",
                    "tags":{
                            "id":self_identifier,
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
