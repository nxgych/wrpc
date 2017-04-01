#coding:utf-8

'''
@author: shuai.chen
Created on 2017年3月4日
'''
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod
import logging
import threading

from kazoo.exceptions import SessionExpiredError
from kazoo.recipe.watchers import ChildrenWatch

from wrpc.common import constant, util, WrpcException
from wrpc.common.node import ServerNode
from .load_balance import RoundRobinLoad

logger = logging.getLogger(__name__)

class Provider(object):
    
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def select(self):
        raise NotImplementedError

    @abstractmethod
    def get_service_ifaces(self):
        raise NotImplementedError
    
    def listen(self):
        pass
    
    def set_client_pool(self, client_pool):
        pass
    
    def close(self):
        pass   

class AutoProvider(Provider):
    
    __lock = threading.RLock()
    
    def __init__(self, zk_client, global_service, version=constant.VERSION_DEFAULT,
                 service_ifaces = [], load_balance = RoundRobinLoad):
        """
        auto server provider listened by ChildrenWatch
        @param zkclient: zookeeper connection client
        @param global_service: global service name
        @param version: server version default is 1.0.0
        @param service_ifaces: service ifaces class
        @param load_balance: load balance class, RoundRobinLoad or RandomLoad, 
                             default is RoundRobinLoad
        """
        try:
            self.__zk_client = zk_client 
        except SessionExpiredError:
            logger.warn("Zookeeper Client session timed out.")    
            
        self.__global_service = global_service
        self.__version = version   
        self.__service_ifaces = service_ifaces
        self.__load_balance = load_balance()
        
        self.__all_nodes = set()
        self.__live_nodes = set()
        
        self.client_pool = None
        
    def __get_parent_path(self):
        return "{0}{1}{2}{3}".format(                                   
                                    constant.ZK_SEPARATOR_DEFAULT,
                                    self.__global_service,
                                    constant.ZK_SEPARATOR_DEFAULT,
                                    self.__version
                                    )
    
    def listen(self):
        if self.__zk_client:
            ChildrenWatch(self.__zk_client, self.__get_parent_path(), func=self.__watch_server_node)
        
    def __watch_server_node(self, nodes):
        with self.__lock:
            self.__live_nodes.clear()
            
            if len(nodes) <= 0:
                logger.warn("server not found!") 
                            
            for node in nodes:
                server_node = ServerNode(node)
                self.__all_nodes.add(server_node)
                self.__live_nodes.add(server_node)  
                
            self.__load_balance.set_nodes(self.__live_nodes)    
            if self.client_pool is not None:
                self.client_pool.clear_pool()    
            
        logger.info("Child node changed.")        
    
    def select(self):
        return self.__load_balance.get_node()
    
    def get_service_ifaces(self):
        return self.__service_ifaces
    
    def set_client_pool(self, client_pool):
        self.client_pool = client_pool
        
    def close(self):
        if self.__zk_client:
            self.__zk_client.close()

class FixedProvider(Provider):
    
    def __init__(self, server_address, global_service, version=constant.VERSION_DEFAULT,
                 service_ifaces = [], load_balance = RoundRobinLoad):
        """
        fixed server provider
        @param server_address: server adress  as string 'ip:port:weight' or 'ip:port'     
        @param global_service: global service name
        @param version: server version default is 1.0.0        
        @param service_ifaces: service ifaces class
        @param load_balance: load balance class, RoundRobinLoad or RandomLoad, 
                             default is RoundRobinLoad
        """   
        if not util.check_hosts(server_address):
            raise WrpcException("Illegal server_address for FixedProvider.")        
             
        self.__global_service = global_service
        self.__server_address = server_address
        self.__service_ifaces = service_ifaces
        self.__load_balance = load_balance()

        self.__all_nodes = set()
        self.__live_nodes = set()
        
        self.__set_nodes()
        
    def __set_nodes(self):
        nodes = self.__server_address.split(",")
        for node in nodes:
            server_node = ServerNode(node)
            self.__all_nodes.add(server_node)
            self.__live_nodes.add(server_node)  
        
        self.__load_balance.set_nodes(self.__live_nodes)       
    
    def select(self):
        return self.__load_balance.get_node()
    
    def get_service_ifaces(self):
        return self.__service_ifaces
            