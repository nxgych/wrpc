#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
@author: shuai.chen
Created on 2017年3月4日
'''
from __future__ import absolute_import

import functools
import logging
import time
import types

from thrift.transport.TTransport import TTransportException
from wrpc.common.proxy import Proxy
from wrpc.common.pool import KeyedObjectPool
from wrpc.common import WrpcException

from .factory import ThriftClientFactory

logger = logging.getLogger(__name__)

class Client(object):
    """client class"""
    
    def __init__(self, provider, client_class=ThriftClientFactory, 
                 retry=3, retry_interval=0.2, **kwargs):
        """
        client 
        @param provider: server provider,instance of AutoProvider or FixedProvider class
        @param client_class: child class of ClientFactory, default is ThriftClientFactory
        @param retry: retry access times, default is 3
        @param retry_interval: retry interval time, default 0.2s
        @param kwargs: 
            pool_max_size: client pool max size, default is 8    
            pool_wait_timeout: client pool block time, default is None means forever      
        """
        self.__provider = provider
        self.__proxy_map = {}

        self.__set_client_pool(client_class, **kwargs)  
        self.__add_client_proxy(retry, retry_interval)     
        self.__listen()
            
    def __set_client_pool(self, client_class, **kwargs):
        service_ifaces = self.__provider.get_services()
        ifaces = {iface.__name__.split(".")[-1]:iface for iface in service_ifaces}
        client_factory = client_class(self.__provider, ifaces)   
        self.__client_pool = ClientPool(client_factory, **kwargs)     
        
    def __add_client_proxy(self, retry, retry_interval):   
        service_ifaces = self.__provider.get_services()
        pool = self.__client_pool
        for iface in service_ifaces:
            service_name = iface.__name__.split(".")[-1]
            self.__proxy_map[service_name] = ClientProxy(service_name, pool, retry, retry_interval)     

    def __listen(self):    
        self.__provider.set_client_pool(self.__client_pool)
        self.__provider.listen()
        time.sleep(1)
        
    def close(self):
        if self.__provider:
            self.__provider.close()   

    def get_client(self, skey):
        """
        get service object
        @param skey: service module or module name
        @use:
            service = client.get_client('MessageService') ||
            service = client.get_client(MessageService)
            
            result = service.sendSMS('10086')
        """  
        key = skey.__name__.split(".")[-1] if type(skey) == types.ModuleType else skey
        return self.__proxy_map.get(key)
        
    def __call__(self, skey, fun, *args):
        '''
        call service function
        @param skey: service module or module name
        @param fun: service function or function name
        @param args: args of service function  
        @use:
            result = client("MessageService", "sendSMS", '10086') ||
            result = client(MessageService, MessageService.Iface.sendSMS, '10086')    
        '''         
        return self.call(skey, fun, *args)
    
    def call(self, skey, fun, *args):
        '''
        call service function
        @param skey: service module or module name
        @param fun: service function or function name
        @param args: args of service function  
        @use:
            result = client.call("MessageService", "sendSMS", '10086') ||
            result = client.call(MessageService, MessageService.Iface.sendSMS, '10086')    
        '''        
        client_proxy = self.get_client(skey)
        func_name = fun.__name__ if callable(fun) else fun
        func = getattr(client_proxy, func_name)
        return func(*args)

    def get_func(self, skey, fun):
        """
        get function object
        @param skey: service module or module name
        @param fun: service function or function name
        @use:
            func = client.get_func("MessageService", "sendSMS") ||
            func = client.get_func(MessageService, MessageService.Iface.sendSMS)
            
            result = func('10086')
        """
        return functools.partial(self.call, skey, fun)
    
class ClientPool(object):
    """client pool"""
    
    def __init__(self, client_factory, **kwargs):
        self.pool = KeyedObjectPool(client_factory.create, **kwargs)
            
    def get_pool(self):
        return self.pool
 
    def clear_pool(self):
        self.pool.clear() 
        logger.info("Client pool cleared.")  
        
class ClientProxy(Proxy):
    '''client proxy class'''
    
    def __init__(self, service_name, pool, retry, retry_interval): 
        '''
        @param service: service name
        @param pool: client pool  
        @param retry: retry access times, default is 3
        @param retry_interval: retry interval time, default 0.2s        
        '''
        self.service_name = service_name
        self.pool = pool   
        self.retry = retry
        self.retry_interval = retry_interval

    def call(self, fun, *args):
        '''
        @param fun: function name
        @param args: args of service function        
        '''
        key = self.service_name
        exception = None
        for _ in range(self.retry):
            obj = None
            flag = True
            try:
                obj = self.pool.get_pool().borrow_obj(key)
                if not hasattr(obj, fun):
                    raise WrpcException("Unknown method!")
                
                func = getattr(obj, fun)
                return func(*args)
            except TTransportException as e:
                exception = e
                flag = False
                logger.error("Could not connect server!")
            except Exception as e:
                exception = e
                flag = False
            finally:
                if obj is not None:
                    if flag:
                        self.pool.get_pool().return_obj(obj, key)  
                    else:
                        self.pool.get_pool().destroy_obj(obj, key)      
            
            time.sleep(self.retry_interval)                
        raise exception   
    