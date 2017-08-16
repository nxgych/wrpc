#-*- coding: utf-8 -*-

'''
@author: shuai.chen
Created on 2017年3月4日
'''
from __future__ import absolute_import

import threading

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from .common import constant, util, WrpcException 

class ZkClient(KazooClient):
    """
    zookeeper client factory
    """
    
    __client = None
    __lock = threading.RLock()
    
    def __init__(self, hosts="127.0.0.1:2181", timeout=10, namespace="", **kwargs):
        '''
        @param hosts: zookeeper hosts 
        @param timeout: timeout  
        @param namespace: chroot
        '''        
        if not util.check_hosts(hosts):
            raise WrpcException("Illegal hosts for zookeeper connection.")
        
        self.namespace = self.__get_namespace(namespace)
        zk_hosts = "{0}{1}".format(hosts, self.namespace)
        connection_retry = KazooRetry(max_tries=-1, max_delay=60)
        super(ZkClient, self).__init__(zk_hosts, timeout, connection_retry=connection_retry, **kwargs)
        
    def __del__(self):
        self.close()    

    def __get_namespace(self, namespace):
        s, r = constant.ZK_SEPARATOR_DEFAULT, constant.ZK_ROOT
        if not namespace:
            return "{0}{1}".format(s, r)
        return "{0}{1}{2}{3}".format(s, r, s, namespace)
        
    @classmethod
    def make(cls, hosts="127.0.0.1:2181", timeout=10, namespace="", **kwargs):
        """
        直接创建zookeeper客户端并启动
        @param hosts: zookeeper hosts 
        @param timeout: timeout  
        @param namespace: chroot
        """
        with cls.__lock:
            if cls.__client is None:
                cls.__client = cls(hosts, timeout, namespace, **kwargs)
                cls.__client.start()
            return cls.__client

    def get_namespace(self):
        return self.namespace
                  
    def close(self):
        try:
            if self.__client is not None:
                self.__client.close()
            else:
                super(ZkClient, self).close()   
        except:
            pass         
