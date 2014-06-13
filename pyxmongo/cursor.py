# -*- coding: utf-8 -*-
#14-4-18
# create by: snower

import math

class CursorData(object):
    def __init__(self,cursor,data=None):
        self.__cursor=cursor
        if data is not None:
            self.__data=data
        else:
            self.next()

    def next(self):
        try:
            self.__data=self.cursor.next()
        except StopIteration:
            self.__data=None

    @property
    def data(self):
        return self.__data

    @property
    def cursor(self):
        return self.__cursor

    def __getitem__(self, key):
        return self.__data[key]

class Cursor(object):
    def __init__(self,collection,cursors):
        self.__collection=collection
        self.__cursors=cursors
        self.__limit=-1
        self.__skip=-1
        self.__sort=None
        self.__iter=None
        self.__index=0
        self.__loaded=False

    def limit(self,count):
        self.__limit=count
        return self

    def skip(self,count):
        self.__skip=count
        return self

    def sort(self,key_or_list, direction=None):
        self.__sort=[(key_or_list,direction)] if isinstance(key_or_list,basestring) else key_or_list
        return self

    def _limit(self):
        if self.__limit>=0:
            for cursor in self.__cursors:
                cursor.limit(self.__limit)

    def _skip(self):
        if self.__skip>0:
            if len(self.__cursors)==1:
                self.__cursors[0].skip(self.__skip)
                return
            skip=int(math.sqrt(self.__skip/len(self.__cursors)))
            if skip>10:
                max_skip=self.__skip-(self.__skip % skip)-int(self.__skip/len(self.__cursors))
                skip_count=0
                skip_datas={id(cursor):0 for cursor in self.__cursors}
                datas=sorted([data for data in [CursorData(cursor,cursor[0]) for cursor in self.__cursors] if data.data is not None],self._cmp)
                while skip_count<max_skip:
                    cursor=datas[0].cursor
                    key=id(cursor)
                    skip_datas[key]+=skip
                    datas[0]=CursorData(cursor,cursor[skip_datas[key]])
                    skip_count+=skip
                    datas=sorted(datas,self._cmp)
                for cursor in self.__cursors:
                    if skip_datas[id(cursor)]!=0:cursor.skip(skip_datas[id(cursor)])
                self.__skip -= skip_count
            self.__limit+=self.__skip

    def _sort(self):
        if self.__sort:
            for cursor in self.__cursors:
                cursor.sort(self.__sort)

    def _cmp(self,x,y):
        if x.data is None:return 1
        if y.data is None:return -1
        if self.__sort is None:return -1
        c,sort=0,None
        for sort in self.__sort:
            c=cmp(x[sort[0]],y[sort[0]])
            if c!=0:break
        return c if sort and sort[1]==1 else -c

    def get_current_data(self,datas):
        if not datas:return None
        data=datas.pop(0)
        while data.data is None:
            if not datas:return None
            data=datas.pop(0)
        if not datas:
            datas.append(data)
            return data

        start,end,index=0,len(datas),0
        while end-start!=1:
            index=int(start+(end-start)/2)
            c=self._cmp(data,datas[index])
            if c>=0:start=index
            elif c<0:end=index
        if start==0:
            c=self._cmp(data,datas[0])
            if c<0:index=-1
        datas.insert(index+1,data)
        return datas[0]

    def __iter__(self):
        return self

    def next(self):
        if not self.__loaded:
            self._sort()
            self._skip()
            self._limit()
            self.__loaded=True

        if self.__iter is None:
            self.__index=0
            def iter():
                if len(self.__cursors)==1:
                    for data in self.__cursors[0]:
                        yield data
                else:
                    datas=sorted([data for data in [CursorData(cursor) for cursor in self.__cursors] if data.data is not None],self._cmp)
                    data=self.get_current_data(datas)
                    while (self.__limit==-1 or self.__index<self.__limit) and data and data.data:
                        if self.__skip==-1 or self.__index>=self.__skip:
                            yield data.data
                        self.__index+=1
                        data.next()
                        data=self.get_current_data(datas)
            self.__iter=iter()
        return self.__iter.next()

    def count(self):
        count=0
        for cursor in self.__cursors:
            count+=cursor.count()
        return count

    def rewind(self):
        for cursor in self.__cursors:
            cursor.rewind()
        self.__iter=None

    def close(self):
        for cursor in self.__cursors:
            cursor.close()