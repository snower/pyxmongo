# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

from xmongo_client import MongoClient

class Connection(MongoClient):
    def __init__(self,config):
        super(Connection,self).__init__(config)