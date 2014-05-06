# -*- coding: utf-8 -*-
#14-4-17
# create by:snower

test={
    "hosts":[
        {"host":"127.0.0.1"},
    ],
    "databases":{
        "test":{
            "slice":"int_slice",
            "slice_params":{
                "key":"id",
                "format":"test_%s",
                "mod":4
            },
            "collections":{
                "test":{"slice":"int_slice",
                    "slice_params":{
                        "key":"id",
                        "format":"test_%s",
                        "mod":16
                    }
                }
            }
        }
    }
}
