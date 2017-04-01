#coding:utf-8

'''
@author: shuai.chen
Created on 2017年3月5日
'''

import random
import threading

from abc import ABCMeta,abstractmethod
from wrpc.common import WrpcException

class LoadBalance(object):
    """
    load balance base
    """
    
    __metaclass__ = ABCMeta
    
    def __init__(self, nodes=[]):
        self._nodes = self._transfer(nodes)    
    
    @abstractmethod
    def get_node(self):
        raise NotImplementedError
    
    def set_nodes(self, nodes):
        self._nodes = self._transfer(nodes)
    
    def _transfer(self, nodes):
        node_list = []
        for node in nodes:
            for _ in xrange(node.weight):
                node_list.append(node)
        random.shuffle(node_list)        
        return node_list

class RandomLoad(LoadBalance):
    """
    random load balance
    """
    
    def get_node(self):
        if len(self._nodes) <= 0:
            raise WrpcException("Server not found!")
        return random.choice(self._nodes)
  
class RoundRobinLoad(LoadBalance):
    """
    round robin load balance
    """
    
    __lock = threading.RLock()
    
    __pos = 0
    
    def get_node(self): 
        size = len(self._nodes)
        if size<= 0:
            raise WrpcException("Server not found!")
                 
        with self.__lock:
            if self.__pos >= size:
                self.__pos = 0
                
            node = self._nodes[self.__pos]  
            self.__pos += 1
            return node
                        