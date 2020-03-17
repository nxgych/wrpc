#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
@author: shuai.chen
Created on 2017年3月4日
'''

import logging
import threading

from kazoo.client import KazooState
from kazoo.exceptions import SessionExpiredError

logger = logging.getLogger(__name__)

class Register(object):
    """register server"""
    
    __lock = threading.RLock()
    
    def __init__(self, server):
        self.__server = server  
                
    def _register(self):
        with self.__lock:
            try:
                zk_client = self.__server.zk_client
                server_config = self.__server.server_config
                
                path = ""
                if server_config.get_ip() is not None:
                    path = server_config.get_path()
                else:
                    old_path = server_config.get_path()
                    #reset ip
                    server_config.set_local_ip()  
                    path = server_config.get_path()
                    #delete old path if ip changed
                    if(path != old_path):
                        if zk_client.exists(old_path):   
                            zk_client.delete(old_path)
                            logger.info("delete old path: %s" % old_path)  
  
                logger.info("prelook register path: %s" % path);
                if not zk_client.exists(path):
                    zk_client.create(path, ephemeral=True)   
                    logger.info("registe server success.");
            except SessionExpiredError:
                logger.warn("Zookeeper Client session timed out.") 
            except Exception:
                logger.error("Server register error!")   
                
    def register_and_listen(self):
        zk_client = self.__server.zk_client
        if zk_client is None:
            return 
        
        server_config = self.__server.server_config
        zk_client.ensure_path(server_config.get_parent_path())        
        self._register()
        
        def state_listener(state): 
            if state == KazooState.CONNECTED:
                logger.info("Connection is connected.")
                #register check
                zk_client.handler.spawn(self._register)
            elif state == KazooState.LOST:
                logger.warn("Connection is lost!")
            elif state == KazooState.SUSPENDED:
                logger.info("Connection is suspended.")
            else:
                pass
 
        zk_client.add_listener(state_listener)
    
    def close(self):
        if self.__server:
            self.__server.stop()    
