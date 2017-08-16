#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月16日
@author: shuai.chen
'''

from os.path import join, dirname
from setuptools import setup,find_packages

def get_file_contents(filename):
    with open(join(dirname(__file__), filename)) as fp:
        return fp.read()

def get_install_requires():
    requirements = get_file_contents('requirements.txt')
    install_requires = []
    for line in requirements.split('\n'):
        line = line.strip()
        if line and not line.startswith('-'):
            install_requires.append(line)
    return install_requires

setup(name='wrpc',  #打包后的包文件名  
      version='1.0.0',    
      description="""wrpc is a lightweight rpc framework based thrift and zookeeper, 
                     it is cross language and support java and python at present.""",    
      long_description=get_file_contents('README.md'),
      author='shuai.chen',    
      author_email='nxgych@163.com',    
      url='https://github.com/nxgych/wrpc',
      packages=find_packages(exclude=['test']),    
      include_package_data=True,
      install_requires=get_install_requires()
)   