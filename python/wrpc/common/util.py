#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
@author: shuai.chen
Created on 2017年3月4日
'''

import platform
import re
import socket
import struct
import fcntl

from wrpc.common import UncheckedException

def singleton(cls):  
    """
    singleton decorator
    """
    _instances = {}  
    def _singleton(*args, **kwargs):  
        if cls not in _instances:  
            _instances[cls] = cls(*args, **kwargs)  
        return _instances[cls]  
    return _singleton  

def unchecked(cls):
    """
    decorator for class which is unchecked and can't be used at present
    """
    def _deco(*args, **kwargs):
        raise UncheckedException(cls.__name__)
    return _deco

def get_clazz_string(clazz):   
    '''
    get class name string
    @param clazz: class
    '''
    class_string = str(clazz)
    if "'" in class_string:
        pat = "(?<=\\').+?(?=\\')"
        search = re.search(pat, class_string)
        if search:
            return search.group(0)
    return class_string

def get_local_ip():
    """
    get local ip
    """
    def get_ip():
        _system = platform.system()
        if _system == "Linux":
            soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            inet = fcntl.ioctl(soc.fileno(), 0x8915, struct.pack('256s', 'eth0')) 
            return socket.inet_ntoa(inet[20:24]) 
        return socket.gethostbyname(socket.gethostname())
    
    try:
        return get_ip()
    except:
        return get_ip()

def check_hosts(hosts):
    """
    check hosts config
    """
    pat = r"^\d{1,3}(.\d{1,3}){3}(:\d+){1,2}(,\d{1,3}(.\d{1,3}){3}(:\d+){1,2})*$" 
    return re.match(pat,hosts)
