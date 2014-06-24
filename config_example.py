# -*- coding: utf-8 -*-
#14-4-17
# create by:snower'

'''
配置一个collection conn.test.test，以id模2选择服务器，模4选择database，模16选择collection
'''

test={
    "hosts":[
        {"host":"192.168.1.2"},
        {"host":"192.168.1.3"},
    ],
    "slice":"int_slice",
    "slice_params":{
        "key":"id",
        "format":"%s", #format只能是"%s"  hosts[int(format)]
        "mod":2
    },
    "databases":{
        "test":{
            "slice":"int_slice",
            "slice_params":{
                "key":"id",
                "format":"test_%s", #database名 "test_"+id % mod
                "mod":4
            },
            "collections":{
                "test":{
                    "slice":"int_slice",
                    "slice_params":{
                        "key":"id",
                        "format":"test_%s", #collection名 "test_"+id % mod
                        "mod":16
                    }
                }
            }
        }
    }
}
