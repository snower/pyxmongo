# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

from pymongo.connection import Connection as PyConnection
from pymongo.collection import *
from common import BaseObject
from slices import Slice
from cursor import Cursor

class Collection(BaseObject):
    def __new__(cls, database,*args, **kwargs):
        from database import Database
        if isinstance(database,Database):
            return object.__new__(cls,database,*args, **kwargs)
        collection=object.__new__(PyConnection,database,*args, **kwargs)
        collection.__init__(database,*args, **kwargs)
        return collection

    def __init__(self,database,name):
        super(Collection,self).__init__()
        self.__name=name
        self.__database=database
        self.__config=database.get_config(name)
        self.__slice=self.__init_slice()
        self.__collections={}

    @property
    def database(self):
        return self.__database

    @property
    def name(self):
        return self.__name

    def __init_slice(self):
        if "slice" not in self.__config:
            slice=Slice.get_slice("")
            return slice(self.__name)
        slice=Slice.get_slice(self.__config["slice"])
        return slice(**self.__config["slice_params"])

    def __run_command(self,data,cmd,*args,**kwargs):
        rets=[]
        for conn,db_name,collection_name in self.get(data):
            rets.append(getattr(conn[db_name][collection_name],cmd)(*args,**kwargs))
        return rets

    def __run_command_all(self,cmd,*args,**kwargs):
        rets=[]
        for name,collections in self.select().iteritems():
            for collection in collections:
                rets.append(getattr(collection,cmd)(*args,**kwargs))
        return rets

    def load_info(self):
        self.__collections={}
        for name,dbs in self.__database.select().iteritems():
            for db in dbs:
                for collection in db.collection_names():
                    if self.__slice.check(collection):
                        if collection not in self.__collections:
                            self.__collections[collection]=[]
                        self.__collections[collection].append(db[collection])

    def select(self,name=None):
        self.load_info()
        if name:
            return self.__collections[name]
        return self.__collections

    def get(self,data):
        for collection_name in self.__slice.select(data):
            for conn,db_name in self.__database.get(data):
                yield conn,db_name,collection_name
        raise StopIteration

    def insert(self,datas):
        if isinstance(datas,dict):
            return self.__run_command(datas,"insert",datas)[0]
        else:
            collections={}
            slice_datas={}
            rets=[]
            for data in datas:
                for conn,db_name,collection_name in self.get(data):
                    key=str((conn,db_name,collection_name))
                    if key not in collections:
                        collections[key]=(conn,db_name,collection_name)
                        slice_datas[key]=[]
                    slice_datas[key].append(data)
            for key in collections:
                conn,db_name,collection_name=collections[key]
                collection=conn[db_name][collection_name]
                rets.extend(collection.insert(slice_datas[key]))
            return rets

    def save(self,to_save,*args,**kwargs):
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))
        return self.__run_command(to_save,"save",to_save,*args,**kwargs)[0]

    def update(self,spec,*args,**kwargs):
        return self.__run_command(spec,"update",spec,*args,**kwargs)[0]

    def drop(self,*args,**kwargs):
        self.__run_command_all("drop",*args,**kwargs)

    def find_and_modify(self, query={},*args,**kwargs):
        return self.__run_command(query,"find_and_modify",query,*args,**kwargs)[0]

    def find(self,spec_or_id=None,*args,**kwargs):
        if spec_or_id is not None and not isinstance(spec_or_id,dict):
            spec_or_id={"_id":spec_or_id}
        if not spec_or_id:
            return Cursor(self,self.__run_command_all("find",spec_or_id,*args,**kwargs))
        return Cursor(self,self.__run_command(spec_or_id,"find",spec_or_id,*args,**kwargs))

    def find_one(self,query_or_id=None,*args,**kwargs):
        if query_or_id is not None and not isinstance(query_or_id,dict):
            query_or_id={"_id":query_or_id}
        if not query_or_id:
            rets=self.__run_command_all("find_one",query_or_id,*args,**kwargs)
        else:
            rets=self.__run_command(query_or_id,"find_one",query_or_id,*args,**kwargs)
        if not rets:return None
        def _cmp(x,y):
            if x is None or not isinstance(x,dict):return 1
            if y is None or not isinstance(y,dict):return -1
            return cmp(x["_id"],y["_id"])
        rets=sorted(rets,_cmp)
        return rets[0]

    def count(self):
        return sum(self.__run_command_all("count"))

    def remove(self,spec_or_id=None,*args,**kwargs):
        if spec_or_id is not None and not isinstance(spec_or_id,dict):
            spec_or_id={"_id":spec_or_id}
        if not spec_or_id:
            return self.__run_command_all("remove",spec_or_id,*args,**kwargs)
        return self.__run_command(spec_or_id,"remove",spec_or_id,*args,**kwargs)

    def index_information(self):
        return self.__run_command_all("index_information")

    def create_index(self,*args,**kwargs):
        return self.__run_command_all("ensure_index",*args,**kwargs)

    def ensure_index(self,*args,**kwargs):
        return self.__run_command_all("ensure_index",*args,**kwargs)

    def drop_indexes(self):
        self.__run_command_all("drop_indexes")

    def drop_index(self,*args,**kwargs):
        return self.__run_command_all("ensure_index",*args,**kwargs)

    def reindex(self):
        self.__run_command_all("reindex")

    def rename(self,name,new_name):
        collection=self.select(name)
        if collection:
            return collection.rename(name,new_name)

    def group(self,*args,**kwargs):
        return True

    def distinct(self,*args,**kwargs):
        return True

    def inline_map_reduce(self,*args,**kwargs):
        return True

    def map_reduce(self,*args,**kwargs):
        return True

    def aggregate(self,*args,**kwargs):
        return True

    def parallel_scan(self,*args,**kwargs):
        return True

    def options(self,*args,**kwargs):
        return True