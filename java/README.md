# wrpc

## Description</br>
    wrpc是一个基于thrift、zookeeper的跨语言、高可用、轻量级rpc框架。
    欢迎同行交流及指正，wechat/qq：506001974

## Feature</br>
    1、基于thrift，跨语言，目前支持java、python、go；
    2、高可用，利用zookeeper集群来管理服务；
    3、支持多service。
  
## Installation</br>
    wget https://github.com/nxgych/wrpc/releases/download/1.0.0/wrpc-1.0.0.jar
    
    for maven：
    <dependency>
        <groupId>com.chen.wrpc</groupId>
        <artifactId>wrpc</artifactId>
        <version>1.0.0</version>
    </dependency>  

    mvn install:install-file -DgroupId=com.chen.wrpc  -DartifactId=wrpc -Dversion=1.0.0 -Dfile=/wrpc-1.0.0.jar -Dpackaging=jar -DgeneratePom=true
     
## Tutorial</br>
    参考test模块。   