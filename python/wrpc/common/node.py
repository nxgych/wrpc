#coding:utf-8

'''
Created on 2017年3月9日
@author: shuai.chen
'''

from wrpc.common import constant
            
class ServerNode(object):
    
    def __init__(self, node):
        """
        server node object
        @param node: string as 'ip:port:weight' or 'ip:port'
        """
        address, port, weight = self.__transfer(node)
        
        self.address = address
        self.port = port
        self.weight = weight
        
    def __getitem__(self, name):
        return getattr(self, name)
    
    def __setitem__(self, name, value):
        setattr(self, name, value) 
        
    def __transfer(self, node):
        split = node.split(":")
        if len(split) == 2:
            return (split[0], int(split[1]), constant.WEIGHT_DEFAULT)
        return (int(e) if e.isdigit() else e for e in split)  
            