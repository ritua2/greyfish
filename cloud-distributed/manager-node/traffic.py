#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request
import os
import redis

import base_functions as bf


app = Flask(__name__)


URL_BASE = os.environ["URL_BASE"]
REDIS_AUTH = os.environ["REDIS_AUTH"]



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
    r_users = redis.Redis(host=URL_BASE, password=os.environ['REDIS_AUTH'], db=5)
    if r_users.get(toktok) != None:
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

        r_users.set(toktok, "Active")

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
        return "INVALID key, cannot create a new user"

    user_action = bf.idb_writer('greyfish')

    r_users = redis.Redis(host=URL_BASE, password=os.environ['REDIS_AUTH'], db=5)
    if r_users.get(toktok) == None:
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

        r_users.delete(toktok)

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

    r_nodes = redis.Redis(host=URL_BASE, password=os.environ['REDIS_AUTH'], db=6)
    if r_nodes.exists(IP_addr):
        return "Node already attached"

    try:
        bf.cluster_action_log(IP_addr, IP_addr, "Attached new storage node", str(MAX_STORAGE)+" KB", NODE_KEY)
        r_nodes.hmset(IP_addr, {"type":"storage", "max storage (KB)":MAX_STORAGE, "available storage (KB)":MAX_STORAGE, "NODE_KEY":NODE_KEY})

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

    r_nodes = redis.Redis(host=URL_BASE, password=os.environ['REDIS_AUTH'], db=6)
    if not r_nodes.exists(node_IP):
        return "Node is not attached to cluster"

    # Checks the node key
    if r_nodes.hget(node_IP, "NODE_KEY").decode("UTF-8") != NODE_KEY:
        return "INVALID, incorrect node key"

    try:
        bf.cluster_action_log(IP_addr, node_IP, "Removed storage node as is")
        r_nodes.delete(node_IP)

        return "Node removed as is"
    except:
        return "INVALID, Server Error: Could not connect to database"



if __name__ == '__main__':
   app.run()
