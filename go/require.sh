#!/bin/bash

echo "install dependences ..."

go get -u github.com/samuel/go-zookeeper/zk
go get -u github.com/apache/thrift/lib/go/thrift

echo "finished!"