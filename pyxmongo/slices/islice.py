# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

from ..slices import Slice as BaseSlice

@BaseSlice.register("int_slice")
class Slice(BaseSlice):
    def __init__(self,mod,*args,**kwargs):
        super(Slice,self).__init__(*args,**kwargs)

        self.__mod=mod
        self.__names=[self.format(i) for i in range(mod)]

    def check(self,name):
        return name in self.__names

    def select(self,data):
        if data is None or self.__key is None:return self.__names
        if self.__key not in data or not isinstance(data[self.__key],int):return self.__names
        return [self.format(data[self.__key] % self.__mod)]

    def format(self,value):
        return self.__format % value

    def isinstance(self,data):
        return isinstance(data[self.__key],int)