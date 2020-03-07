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
    
    def __init__(self, zk_client=None):
        try:
            self.__zk_client = zk_client 
        except SessionExpiredError:
            logger.warn("Zookeeper Client session timed out.")    
                
    def _register(self, server_config):
        with self.__lock:
            try:
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
                        if self.__zk_client.exists(old_path):   
                            self.__zk_client.delete(old_path)
                            logger.info("delete old path: %s" % old_path)
    
                logger.info("prelook register path: %s" % path);
                if not self.__zk_client.exists(path):
                    self.__zk_client.create(path, ephemeral=True)   
                    logger.info("registe server success.");
            except SessionExpiredError:
                logger.warn("Zookeeper Client session timed out.") 
            except Exception:
                logger.error("Server register error!")   
                
    def register_and_listen(self, server_config):
        if self.__zk_client is None:
            return 

        self.__zk_client.ensure_path(server_config.get_parent_path())        
        self._register(server_config)
        
        def state_listener(state): 
            if state == KazooState.CONNECTED:
                logger.info("Connection is connected.")
                #register check
                self.__zk_client.handler.spawn(self._register, server_config)
            elif state == KazooState.LOST:
                logger.warn("Connection is lost!")
            elif state == KazooState.SUSPENDED:
                logger.info("Connection is suspended.")
            else:
                pass
 
        self.__zk_client.add_listener(state_listener)
    
    def close(self):
        if self.__zk_client:
            self.__zk_client.close()    
