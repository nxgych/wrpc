#coding:utf-8

'''
@author: shuai.chen
Created on 2017年3月4日
'''
from __future__ import absolute_import

import time
import functools
import logging
import types

from thrift.transport.TTransport import TTransportException

from wrpc.common.pool import KeyedObjectPool
from .factory import ThriftClientFactory

logger = logging.getLogger(__name__)

class ClientProxy(object):
    """client proxy class"""
    
    def __init__(self, provider, client_class=ThriftClientFactory, 
                 retry=3, retry_interval=0.2, **kwargs):
        """
        client proxy
        @param provider: server provider,instance of AutoProvider or FixedProvider class
        @param client_class: child class of ClientFactory, default is ThriftClientFactory
        @param retry: retry access times, default is 3
        @param retry_interval: retry interval time, default 0.2s
        @param kwargs: 
            pool_max_size: client pool max size, default is 8    
            pool_wait_timeout: client pool block time, default is None means forever      
        """
        self.__provider = provider
        self.__retry = retry
        self.__retry_interval = retry_interval
        
        self.__set_client_pool(client_class, **kwargs)       
        self.__listen()
            
    def __set_client_pool(self, client_class, **kwargs):
        service_ifaces = self.__provider.get_service_ifaces()
        ifaces = {iface.__name__.split(".")[-1]:iface for iface in service_ifaces}
        client_factory = client_class(self.__provider, ifaces)   
        self.__client_pool = ClientPool(client_factory, **kwargs)         

    def __listen(self):    
        self.__provider.set_client_pool(self.__client_pool)
        self.__provider.listen()
        time.sleep(1)
            
    def get_pool(self):
        return self.__client_pool.get_pool()
    
    def __call__(self, skey, fun, *args):
        return self.call(skey, fun, *args)
    
    def call(self, skey, fun, *args):
        """
        直接调用服务端接口
        @param skey: service module or module name
        @param fun: service function or function name
        @param args: args of service function  
        @use:
            result = client.call("MessageService", "sendSMS", '10086') ||
            result = client.call(MessageService, MessageService.Iface.sendSMS, '10086')
        """
        key = skey.__name__.split(".")[-1] if type(skey) == types.ModuleType else skey
        
        exception = None
        for _ in xrange(self.__retry):
            obj = None
            flag = True
            try:
                obj = self.get_pool().borrow_obj(key)
                func = getattr(obj, fun.__name__) if callable(fun) else getattr(obj, fun)
                return func(*args)
            except TTransportException, e:
                exception = e
                flag = False
                logger.error("Could not connect server!")
            except Exception, e:
                exception = e
                flag = False
            finally:
                if obj is not None:
                    if flag:
                        self.get_pool().return_obj(obj, key)  
                    else:
                        self.get_pool().destroy_obj(obj, key)      
            
            time.sleep(self.__retry_interval)                
        raise exception    
    
    def get_service(self, skey):
        """
        get service object
        @param skey: service module or module name
        @use:
            service = client.get_service("MessageService") ||
            service = client.get_service(MessageService)
            
            result = service("sendSMS", "10086") || 
            result = service(MessageService.Iface.sendSMS, "10086")
        """  
        return functools.partial(self.call, skey)

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
