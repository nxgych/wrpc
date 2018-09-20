#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月14日
@author: shuai.chen
'''
from __future__ import absolute_import

import functools
import signal
import threading
from abc import ABCMeta,abstractmethod

from thrift.protocol import TCompactProtocol
from thrift.server import TProcessPoolServer,TNonblockingServer
from thrift.transport import TSocket, TTransport

from thrift import TTornado
from tornado import ioloop

from wrpc.common.util import unchecked

class ServerFactory(object):
    """abstract class of server factory"""
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def start(self):
        raise NotImplementedError
    
    @abstractmethod
    def stop(self):
        raise NotImplementedError
    
class ThriftNonblockingServer(ServerFactory): 
    """基于TNonblockingServer的非阻塞server"""
    
    def __init__(self, processor, ip, port, **kwargs):
        """
        thrift process pool server
        @param processor: thrift processor object
        @param ip: ip 
        @param port: server port
        @param threads_num:  threads num
        """
        try:
            self._server = self.__create_server(processor, ip, port)
            threads = kwargs.get("threads_num", 0)
            if threads > 0:
                self._server.setNumThreads(threads)

            def clean_shutdown(signum, frame):
                for worker in self._server.workers:
                    worker.terminate()
                try:
                    threading.Thread(target=self.stop).start()
                except:
                    pass

            def add_clean_shutdown(func):
                @functools.wraps(func)
                def wrapper(*args, **kwargs):
                    clean_shutdown(*args, **kwargs)
                    return func(*args, **kwargs)
                return wrapper

            sigint_handler = signal.getsignal(signal.SIGINT)
            if callable(sigint_handler):
                signal.signal(signal.SIGINT, add_clean_shutdown(sigint_handler))
            else:
                signal.signal(signal.SIGINT, clean_shutdown)
        except Exception:
            raise

    @staticmethod
    def __create_server(processor, ip, port):
        transport = TSocket.TServerSocket(ip, port)   
        pfactory = TCompactProtocol.TCompactProtocolFactory()
        return TNonblockingServer.TNonblockingServer(processor, transport, pfactory)     
    
    def start(self):
        if self._server:
            self._server.serve()             

    def stop(self):
        if self._server:
            self._server.close()  
                
class ThriftProcessPoolServer(ServerFactory):
    """warning: TProcessPoolServer在没有空闲worker的情况下，会发生阻塞"""

    def __init__(self, processor, ip, port, **kwargs):
        """
        thrift process pool server
        @param processor: thrift processor object
        @param ip: ip 
        @param port: server port
        @param process_num:  process num
        """
        try:
            self._server = self.__create_server(processor, ip, port)
            process_num = kwargs.get("process_num", 0)
            if process_num > 0:
                self._server.setNumWorkers(process_num)

            def clean_shutdown(signum, frame):
                for worker in self._server.workers:
                    worker.terminate()
                try:
                    threading.Thread(target=self.stop).start()
                except:
                    pass

            def add_clean_shutdown(func):
                @functools.wraps(func)
                def wrapper(*args, **kwargs):
                    clean_shutdown(*args, **kwargs)
                    return func(*args, **kwargs)
                return wrapper

            sigint_handler = signal.getsignal(signal.SIGINT)
            if callable(sigint_handler):
                signal.signal(signal.SIGINT, add_clean_shutdown(sigint_handler))
            else:
                signal.signal(signal.SIGINT, clean_shutdown)
        except Exception:
            raise

    @staticmethod
    def __create_server(processor, ip, port):
        transport = TSocket.TServerSocket(ip, port)   
        tfactory = TTransport.TFramedTransportFactory()
        pfactory = TCompactProtocol.TCompactProtocolFactory()
        return TProcessPoolServer.TProcessPoolServer(processor, transport, tfactory, pfactory)     
    
    def start(self):
        if self._server:
            self._server.serve()

    def set_post_fork_callback(self, callback):
        self._server.setPostForkCallback(callback)                 

    def stop(self):
        if self._server:
            self._server.stop()     

class GeventProcessPoolServer(ServerFactory):
    """基于gevent的异步非阻塞ProcessPoolServer"""
    
    def __init__(self, processor, ip, port, **kwargs):
        """
        gevent process pool server
        @param processor: thrift processor object
        @param ip: ip 
        @param port: server port
        @param process_num:  process num
        @param coroutines_num: gevent coroutines num
        """        
        try:
            self._server = self.__create_server(processor, ip, port)
            process_num = kwargs.get("process_num", 0)
            if process_num > 0:
                self._server.setNumWorkers(process_num)
            
            coroutines_num = kwargs.get("coroutines_num", 100)
            self._server.setNumCoroutines(coroutines_num)

            def clean_shutdown(signum, frame):
                for worker in self._server.workers:
                    worker.terminate()
                try:
                    threading.Thread(target=self.stop).start()
                except:
                    pass

            sigint_handler = signal.getsignal(signal.SIGINT)
            if callable(sigint_handler):
                def add_clean_shutdown(method):
                    def wrapper(*args, **kwargs):
                        clean_shutdown(*args, **kwargs)
                        return method(*args, **kwargs)
                    return wrapper
                signal.signal(signal.SIGINT, add_clean_shutdown(sigint_handler))
            else:
                signal.signal(signal.SIGINT, clean_shutdown)
        except Exception:
            raise

    @staticmethod
    def __create_server(processor, ip, port):
        from ._gevent import GProcessPoolServer
        transport = TSocket.TServerSocket(ip, port)   
        tfactory = TTransport.TFramedTransportFactory()
        pfactory = TCompactProtocol.TCompactProtocolFactory()
        return GProcessPoolServer(processor, transport, tfactory, pfactory)
    
    def start(self):
        if self._server:
            self._server.serve()

    def set_post_fork_callback(self, callback):
        self._server.setPostForkCallback(callback)

    def stop(self):
        if self._server:
            self._server.stop()   

@unchecked                    
class TornadoProcessPoolServer(ServerFactory):
    """warning: tornado server暂时不能使用"""
    
    def __init__(self, processor, ip, port, **kwargs):
        self._server = self.__create_server(processor, ip, port)
        self._process_num = kwargs.get("process_num", 0)
  
    @staticmethod
    def __create_server(processor, ip, port):
        pfactory = TCompactProtocol.TCompactProtocolFactory()
        server = TTornado.TTornadoServer(processor, pfactory)
        server.bind(port)
        return server
        
    def start(self):   
        if self._server:
            self._server.start(self._process_num)
            ioloop.IOLoop.instance().start()
        
    def stop(self):
        if self._server:
            self._server.stop()
                        