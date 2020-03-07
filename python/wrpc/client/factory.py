#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月14日
@author: shuai.chen
'''

import logging
from functools import partial
from abc import ABCMeta, abstractmethod

from thrift.protocol import TCompactProtocol
from thrift.protocol import TMultiplexedProtocol
from thrift.transport import TSocket, TTransport

logger = logging.getLogger(__name__)

class ClientFactory(object):    
    """abstract class of client factory"""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def create(self, key):
        raise NotImplementedError
            
class ThriftClientFactory(ClientFactory):
    """thrift client factory"""
    
    def __init__(self, provider, ifaces):
        """
        @param provider: server provider,instance of AutoProvider or FixedProvider class
        @param ifaces: ifaces map as {iface name: iface class}         
        """
        self.__provider = provider
        self.__ifaces = ifaces
    
    def create(self, key):
        server_node = self.__provider.select()
        tsocket = TSocket.TSocket(server_node.address, server_node.port)  
        transport = TTransport.TFramedTransport(tsocket)
        protocol = TCompactProtocol.TCompactProtocol(transport)  
        mprotocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol,key)  
        client = getattr(self.__ifaces.get(key), "Client")
        transport.open()
        instance = client(mprotocol)
        
        # transport close function
        def close(self):
            try:
                self._iprot.trans.close()
                self._oprot.trans.close()
                logger.info("Client closed.") 
            except:
                logger.error("Client close error!")   
             
        #add close function
        instance.close = partial(close, instance)    
        
        logger.info("Client created.") 
        return instance

class GeventClientFactory(ThriftClientFactory):
    """gevent client factory"""
