#coding:utf-8

'''
Created on 2017年3月15日
@author: shuai.chen
'''

from wrpc.shortcut import create_server
    
SERVER_CONFIG = {
    "zk_hosts":"127.0.0.1:2181",
    "zk_timeout":5,
    "global_service":"com.wrpc.test",
    "handlers":["test.MessageHandler"]
}

if __name__ == "__main__":
    server = create_server(**SERVER_CONFIG)
    server.start()
    