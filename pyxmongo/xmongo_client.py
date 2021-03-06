# -*- coding: utf-8 -*-
#14-5-6
# create by: snower

import pymongo
from common import BaseObject
from slices import Slice
from database import Database

class MongoClient(BaseObject):
    def __init__(self,config):
        super(MongoClient,self).__init__()
        self.__config=config
        self.__connections=[]
        self.__slice=self.__init_slice()

        self.__connect()

    def __init_slice(self):
        if "slice" not in self.__config:
            slice=Slice.get_slice("int_slice")
            return slice(**{"format":"%s","mod":len(self.__config["hosts"])})
        slice=Slice.get_slice(self.__config["slice"])
        return slice(**self.__config["slice_params"])

    def __connect(self):
        for host in self.__config["hosts"]:
            self.__connections.append(pymongo.Connection(**host))

    def __get_database(self,name):
        if name in self.__config["databases"]:
            return Database(name,self,self.__config["databases"][name])
        return None

    def select(self,index=None):
        if isinstance(index,int):
            return self.__connections[index]
        return self.__connections

    def get(self,data):
        for index in self.__slice.select(data):
            yield self.__connections[int(index)]
        raise StopIteration

    def close(self):
        for connection in self.__connections:
            connection.close()

    def disconnect(self):
        for connection in self.__connections:
            connection.disconnect()

    def database_names(self):
        return self.__config["databases"].keys()

    def drop_database(self,name_or_database):
        if isinstance(name_or_database,basestring) and name_or_database in self.__config["databases"]:
            name_or_database=self[name_or_database]
        for name,databases in name_or_database.select().iteritems():
            for database in databases:
                database.connection.drop_database(name)

    def __getattr__(self, name):
        return self.__get_database(name)

    def __getitem__(self, name):
        return self.__get_database(name)