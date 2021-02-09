#!/usr/bin/env python3
"""
BASICS

Implements communication between end user calling greyfish and the other nodes
"""


from flask import Flask, request
import os, shutil
import redis

import base_functions as bf


app = Flask(__name__)
GREYFISH_DIR = os.environ['greyfish_path']+"/sandbox/"


URL_BASE = os.environ["URL_BASE"]
REDIS_AUTH = os.environ["REDIS_AUTH"]



#################################
# USER ACTIONS
#################################





if __name__ == '__main__':
   app.run()
