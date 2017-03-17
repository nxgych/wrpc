#coding:utf-8

'''
Created on 2017年3月14日
@author: shuai.chen
'''

import logging
from functools import partial
from abc import ABCMeta, abstractmethod

from thrift.TTornado import TTornadoStreamTransport
from thrift.protocol import TCompactProtocol
from thrift.protocol import TMultiplexedProtocol
from thrift.transport import TSocket, TTransport
from tornado import gen

from wrpc.common.util import unchecked

logger = logging.getLogger(__name__)

class ClientFactory(object):    
    """abstract class of client factory"""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def create(self, key):
        raise NotImplemented()
            
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
            except:
                logger.error("Client close error!")   
             
        #add close function
        instance.close = partial(close, instance)    
        return instance

class GeventClientFactory(ThriftClientFactory):
    """gevent client factory"""

@unchecked
class TornadoClientFactory(ClientFactory):
    """warning: tornado client暂时不能使用"""
    
    def __init__(self, provider, ifaces):
        """
        @param provider: server provider,instance of AutoProvider or FixedProvider class
        @param ifaces: ifaces map as {iface name: iface class}         
        """
        self.__provider = provider
        self.__ifaces = ifaces
    
    def create(self, key):
        server_node = self.__provider.select()
        transport = TTornadoStreamTransport(server_node.address, server_node.port)  
        protocol = TCompactProtocol.TCompactProtocolFactory() 
        mprotocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol,key)  
        client = getattr(self.__ifaces.get(key), "Client")
        yield gen.Task(transport.open)
        instance = client(transport, mprotocol)  

        # transport close function
        def close(self):
            try:
                self._iprot.trans.close()
            except:
                logger.error("Client close error!")     
             
        #add close function
        instance.close = partial(close, instance)           
        yield instance
        raise gen.Return()
