#coding:utf-8

'''
Created on 2017年3月15日
@author: shuai.chen
'''

from wrpc.shortcut import create_client
from test.message import MessageService

CLIENT_CONFIG = {
    "zk_hosts":"127.0.0.1:2181",
    "global_service":"com.wrpc.test",
    "service_ifaces":["test.message.MessageService"]
}

if __name__ == "__main__":
    client = create_client(**CLIENT_CONFIG)
#     print client.call("MessageService", "sendSMS", "110")
    print client.call(MessageService, MessageService.Iface.sendSMS, "110")
