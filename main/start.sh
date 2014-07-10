#!/bin/bash

nohup ./bootstrap -redis=redis_node_6008:6008 -flume=flume001.m6:61111,flume002.m6:61112 --queuename=new-log 2>&1 >stdout.log &