#!/bin/bash

nohup ./bootstrap -redis=redis_node_6061.momo.com:6061,redis_node_6062.momo.com:6062,redis_node_6063.momo.com:6063,redis_node_6064.momo.com:6064,redis_node_6065.momo.com:6065,redis_node_6066.momo.com:6066 -zkhost=momo-zk-001.m6:2181,momo-zk-002.m6:2181,momo-zk-003.m6:2181 --queuename=new-log 2>&1 >stdout.log &