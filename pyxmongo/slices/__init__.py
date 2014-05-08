# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

import os

class Slice(object):
    __slices={}

    def __init__(self,format,key=None):
        self.__format=format
        self.__key=key

    def check(self,name):
        return name==self.__format

    def select(self,data):
        return [self.format(data)]

    def format(self,value):
        return self.__format

    def isinstance(self,value):
        return True

    def has_key(self,data):
        return self.__key in data

    @staticmethod
    def register(name):
        def _(cls):
            Slice.__slices[name]=cls
            return cls
        return _

    @staticmethod
    def get_slice(name):
        return Slice.__slices[name]

Slice.register("")(Slice)
for file in os.listdir(os.path.dirname(__file__)):
    if file.endswith("slice.py"):
        __import__("pyxmongo.slices",globals(),locals(),[file[:-3]])
