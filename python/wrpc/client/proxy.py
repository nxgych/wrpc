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
        
    def __listen(self):    
        self.__provider.set_client_pool(self.__client_pool)
        self.__provider.listen()
        time.sleep(1)
                
    def __set_client_pool(self, client_class, **kwargs):
        service_ifaces = self.__provider.get_service_ifaces()
        ifaces = {iface.__name__.split(".")[-1]:iface for iface in service_ifaces}
        client_factory = client_class(self.__provider, ifaces)   
        self.__client_pool = ClientPool(client_factory, **kwargs)         
    
    def get_pool(self):
        return self.__client_pool.get_pool()
    
    def get_func(self, skey, fun):
        """
        get function 代理函数
        @param skey: service module or module name
        @param fun: service function or function name
        @use:
            func = Client.get_func("UserService", "changeName")
            or
            func = Client.get_func(UserService, UserService.Iface.changeName)
            
            func(*args)
        """
        return functools.partial(self.call, skey, fun)
    
    def call(self, skey, fun, *args):
        """
        直接调用服务端接口
        @param skey: service module or module name
        @param fun: service function or function name
        @param args: args of service function  
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

class ClientPool(object):
    """client pool"""
    
    def __init__(self, client_factory, **kwargs):
        self.pool = KeyedObjectPool(client_factory.create, **kwargs)
            
    def get_pool(self):
        return self.pool
 
    def clear_pool(self):
        self.pool.clear() 
        logger.info("Client pool cleared.")  
