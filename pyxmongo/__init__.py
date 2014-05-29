# -*- coding: utf-8 -*-
#14-4-17
# create by: snower

__version__="0.0.1"

import sys
import pymongo
from pymongo import *
from connection import Connection
from mongo_client import MongoClient

def patch_motor():
    from pyxmongo import mongo_client,database,collection,cursor
    sys.modules["pymongo.mongo_client"]=mongo_client
    sys.modules["pymongo.database"]=database
    sys.modules["pymongo.collection"]=collection
    sys.modules["pymongo.cursor"]=cursor
    import motor
    motor.MotorClient.__delegate_class__=mongo_client.MongoClient
    motor.MotorDatabase.__delegate_class__=database.Database
    motor.MotorCollection.__delegate_class__=collection.Collection
    motor.MotorCursor.__delegate_class__=cursor.Cursor