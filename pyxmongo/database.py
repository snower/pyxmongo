# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

from common import BaseObject
from slices import Slice
from collection import Collection

class Database(BaseObject):
    def __init__(self,name,connection,config):
        super(Database,self).__init__()
        self.__name=name
        self.__connection=connection
        self.__config=config
        self.__slice=self.__init_slice()
        self.__databases={}

        self.load_info()

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
            return Collection(name,self,self.__config["collections"][name])
        return None

    def load_info(self):
        self.__databases={}
        for conn in self.__connection.select():
            for db in conn.database_names():
                if self.__slice.check(db):
                    if db not in self.__databases:
                        self.__databases[db]=[]
                    self.__databases[db].append(conn[db])

    def select(self,name=None):
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
        for name,databases in self.__databases.iteritems():
            for database in databases:
                rets.append(database.command(*args,**kwargs))
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
        for name,databases in self.__databases.iteritems():
            for database in databases:
                database.add_user(*args,**kwargs)

    def remove_user(self, *args,**kwargs):
        for name,databases in self.__databases.iteritems():
            for database in databases:
                database.remove_user(*args,**kwargs)

    def authenticate(self,*args,**kwargs):
        succed=True
        for name,databases in self.__databases.iteritems():
            for database in databases:
                succed=succed and database.authenticate(*args,**kwargs)
        return succed

    def logout(self,*args,**kwargs):
        for name,databases in self.__databases.iteritems():
            for database in databases:
                database.logout(*args,**kwargs)

    def __getattr__(self, name):
        return self.__get_collection(name)

    def __getitem__(self, name):
        return self.__get_collection(name)