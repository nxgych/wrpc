#coding:utf-8

'''
Created on 2017年3月15日
@author: shuai.chen
'''

from wrpc.shortcut import init_client
from test.message import MessageService

CLIENT_CONFIG = {
    "zk_hosts":"127.0.0.1:2181",
    "global_service":"com.wrpc.test",
    "service_ifaces":["test.message.MessageService"]
}

if __name__ == "__main__":
    init_client(**CLIENT_CONFIG)
    
    from wrpc.shortcut import Client
#     print Client.call("MessageService", "sendSMS", "110")
    print Client.call(MessageService, MessageService.Iface.sendSMS, "110")
