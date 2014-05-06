# -*- coding: utf-8 -*-
#14-4-17
# create by: snower

__version__="0.0.1"

import pymongo
from pymongo import *
import connection
from xmongo_client import MongoClient

def Connection(config_or_host,*args,**kwargs):
    if isinstance(config_or_host,dict):
        return connection.Connection(config_or_host)
    return pymongo.Connection(config_or_host,*args,**kwargs)