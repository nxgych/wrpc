#!/usr/bin/python
# -*- coding: utf-8 -*-

# Welcome to use wrpc and give your advices to me, thank you!
# email : nxgych@163.com
# github : https://github.com/nxgych/wrpc

'''
Created on 2017年3月7日
@author: shuai.chen
'''
from __future__ import absolute_import

from .zkclient import ZkClient

from .server.factory import ThriftProcessPoolServer
from .server.server import Server
from .server.server import ServerConfig

from .client.factory import ThriftClientFactory
from .client.client import Client

from .manager.load_balance import RoundRobinLoad
from .manager.provider import AutoProvider
from .common import constant

__all__ = ['create_server', 'create_client']

def import_module(cstr):
    """
    import module
    @param cstr: str or list
    """
    split = cstr.split(".") if isinstance(cstr, str) else cstr
    if len(split) <= 1:
        return __import__(cstr[0],None,None)
    return  __import__(".".join(split[:-1]), globals={}, locals={}, fromlist=[split[-2]])    

def get_class(clazz):
    """
    get class
    @param clazz: class or class name
    """
    if isinstance(clazz, str):
        split = clazz.split(".")
        module = import_module(split)
        return getattr(module, split[-1]) if hasattr(module, split[-1]) else module  
    return clazz
            
def create_server(zk_hosts="", zk_timeout=8, namespace="", 
                  global_service="com.wrpc.service", handlers=[], port=constant.PORT_DEFAULT, 
                  version=constant.VERSION_DEFAULT, weight=constant.WEIGHT_DEFAULT, ip=None, 
                  server_class=ThriftProcessPoolServer, **kwargs):
    """
    create server
    @param zk_hosts: zookeeper hosts,'127.0.0.1:2181'
    @param zk_timeout: zookeeper connection timeout   
    @param namespace: zookeeper chroot  
    @param global_service: global service name
    @param handlers: handlers implemented respective interface
    @param port: server port default is 8603
    @param version: server version default is 1.0.0
    @param weight: server weight default is 1
    @param server_class: child class of ServerFactory, default is ThriftProcessPoolServer
    @param ip: ip address if you force config it instead of getting local ip 
    @param kwargs:
        process_num: server process num default is cpu num
        coroutines_num: gevent coroutines num default is 100 if server_class is GeventProcessPoolServer
                        else ignore that
        threads_num: ThriftNonblockingServer threads num if server_class is ThriftNonblockingServer
                     else ignore that
    """
    #server class
    server_clazz = get_class(server_class) 
    
    #handler instances
    _handlers = []
    for handler in handlers:
        clazz = get_class(handler)
        _handlers.append(clazz)    
            
    zk_client = ZkClient.make(zk_hosts, zk_timeout, namespace) if zk_hosts else None
    server_config = ServerConfig(global_service, _handlers, port, version, weight, ip)
    return Server(zk_client, server_config, server_clazz, **kwargs)

def create_client(zk_hosts="", zk_timeout=8, namespace="", 
                provider_class=AutoProvider, server_address="", 
                global_service="com.wrpc.service", version=constant.VERSION_DEFAULT, 
                service_ifaces=[], load_balance=RoundRobinLoad, 
                client_class=ThriftClientFactory, retry=3, retry_interval=0.2, **kwargs):
    """
    create client
    @param zk_hosts: zookeeper hosts if provider is AutoProvider else ignore that
    @param zk_timeout: zookeeper connection timeout if provider is AutoProvider else ignore that   
    @param namespace: zookeeper chroot if provider is AutoProvider else ignore that    
    @param provider_class: server provider class, AutoProvider or FixedProvider class,
                           default is AutoProvider
    @param server_address: server adress as string 'ip:port:weight' if provider is FixedProvider
                           else ignore that
    @param global_service: global service name
    @param version: server version default is 1.0.0
    @param service_ifaces: service interfaces class
    @param load_balance: load balance class, RoundRobinLoad or RandomLoad, 
                         default is RoundRobinLoad   
    @param client_class: child class of ClientFactory, default is ThriftClientFactory    
    @param retry: retry access times, default is 3     
    @param retry_interval: retry interval time, default 0.2s            
    @param kwargs: 
        pool_max_size: client pool max size, default is 8    
        pool_wait_timeout: client pool block time, default is None means forever          
    """
    #provider class
    provider_clazz = get_class(provider_class)
    
    #service ifaces
    ifaces = []
    for iface in service_ifaces:
        iface_class = get_class(iface)
        ifaces.append(iface_class)         

    #load balance class
    load_balance_class = get_class(load_balance)
    
    #provider class
    if provider_clazz == AutoProvider:
        assert zk_hosts
        server_from = ZkClient.make(zk_hosts, zk_timeout, namespace)
    else:
        assert server_address  
        server_from = server_address

    #client class
    client_clazz = get_class(client_class)
                            
    provider = provider_clazz(server_from, global_service, version, ifaces, load_balance_class)
    return Client(provider, client_clazz, retry, retry_interval, **kwargs)    
    