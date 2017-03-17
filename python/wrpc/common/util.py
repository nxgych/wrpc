#coding:utf-8

'''
@author: shuai.chen
Created on 2017年3月4日
'''

import re
import socket

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

def get_local_ip():
    """
    get local ip
    """
    return socket.gethostbyname(socket.gethostname())

def check_hosts(hosts):
    """
    check hosts config
    """
    pat = r"^\d{1,3}(.\d{1,3}){3}(:\d+){1,2}(,\d{1,3}(.\d{1,3}){3}(:\d+){1,2})*$" 
    return re.match(pat,hosts)
