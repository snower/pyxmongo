# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

from pymongo.database import Database as PyDatabase
from pymongo.database import *
from common import BaseObject
from slices import Slice
from collection import Collection

class Database(BaseObject):
    def __new__(cls, connection,*args, **kwargs):
        from mongo_client import MongoClient
        if isinstance(connection,MongoClient):
            return object.__new__(cls,connection,*args, **kwargs)
        database=object.__new__(PyDatabase,connection,*args, **kwargs)
        database.__init__(connection,*args, **kwargs)
        return database

    def __init__(self,connection,name):
        super(Database,self).__init__()
        self.__name=name
        self.__connection=connection
        self.__config=connection.get_config(name)
        self.__slice=self.__init_slice()
        self.__databases={}

    @property
    def connection(self):
        return self.__connection

    @property
    def name(self):
        return self.__name

    def __init_slice(self):
        if "slice" not in self.__config:
            slice=Slice.get_slice("")
            return slice(self.__name)
        slice=Slice.get_slice(self.__config["slice"])
        return slice(**self.__config["slice_params"])

    def __get_collection(self,name):
        if name in self.__config["collections"]:
            return Collection(self,name)
        return None

    def get_config(self,name):
        return self.__config["collections"][name]

    def load_info(self):
        self.__databases={}
        for conn in self.__connection.select():
            for db in conn.database_names():
                if self.__slice.check(db):
                    if db not in self.__databases:
                        self.__databases[db]=[]
                    self.__databases[db].append(conn[db])

    def select(self,name=None):
        self.load_info()
        if isinstance(name,basestring):
            return self.__databases[name]
        return self.__databases

    def get(self,data):
        for db_name in self.__slice.select(data):
            for conn in self.__connection.get(data):
                yield (conn,db_name)
        raise StopIteration

    def command(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.command(*args,**kwargs))
        return rets

    def eval(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.eval(*args,**kwargs))
        return rets

    def validate_collection(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.validate_collection(*args,**kwargs))
        return rets

    def create_collection(self,*args,**kwargs):
        return True

    def previous_error(self,*args,**kwargs):
        return True

    def dereference(self,*args,**kwargs):
        return True

    def profiling_level(self,*args,**kwargs):
        return True

    def last_status(self,*args,**kwargs):
        return True

    def current_op(self,*args,**kwargs):
        return True

    def set_profiling_level(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.set_profiling_level(*args,**kwargs))
        return rets

    def profiling_info(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.profiling_info(*args,**kwargs))
        return rets

    def error(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.error(*args,**kwargs))
        return rets

    def reset_error_history(self,*args,**kwargs):
        rets=[]
        for name,databases in self.select().iteritems():
            for database in databases:
                rets.append(database.reset_error_history(*args,**kwargs))
        return rets

    def collection_names(self):
        return self.__config["collections"].keys()

    def drop_collection(self, name_or_collection):
        if isinstance(name_or_collection,basestring) or name_or_collection in self.__config["collections"]:
            name_or_collection=self[name_or_collection]
        for name,collections in name_or_collection.select().iteritems():
            for collection in collections:
                collection.database.drop_collection(name)

    def add_user(self,*args,**kwargs):
        for name,databases in self.select().iteritems():
            for database in databases:
                database.add_user(*args,**kwargs)

    def remove_user(self, *args,**kwargs):
        for name,databases in self.select().iteritems():
            for database in databases:
                database.remove_user(*args,**kwargs)

    def authenticate(self,*args,**kwargs):
        succed=True
        for name,databases in self.select().iteritems():
            for database in databases:
                succed=succed and database.authenticate(*args,**kwargs)
        return succed

    def logout(self,*args,**kwargs):
        for name,databases in self.select().iteritems():
            for database in databases:
                database.logout(*args,**kwargs)

    def __getattr__(self, name):
        return self.__get_collection(name)

    def __getitem__(self, name):
        return self.__get_collection(name)