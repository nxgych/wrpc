#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on 2017年3月13日
@author: shuai.chen
'''

import logging
from multiprocessing import Process, Semaphore

import gevent.monkey
import gevent.pool
from thrift.server.TProcessPoolServer import TProcessPoolServer

# patch os.fork
gevent.monkey.patch_os()
gevent.monkey.patch_socket()
gevent.monkey.patch_time()
gevent.monkey.patch_ssl()

logger = logging.getLogger(__name__)

class GProcessPoolServer(TProcessPoolServer):
    """
    Server with a fixed size pool of worker subprocesses which has a gevent pool to service requests
    """

    def __init__(self, *args):
        TProcessPoolServer.__init__(self, *args)
        self.numCoroutines = 100
        self.stopCondition = None

    def workerProcess(self, semaphore):
        """Loop getting clients from the shared queue and process them"""
        if self.postForkCallback:
            self.postForkCallback()

        semaphore.release()

        pool = gevent.pool.Pool(self.numCoroutines)

        while self.isRunning.value:
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                pool.spawn(self.serveClient, client)
            except (KeyboardInterrupt, SystemExit):
                return 0
            except Exception, x:
                logger.exception(x)

    def serve(self):
        """Start workers and put into queue"""
        # this is a shared state that can tell the workers to exit when False
        self.isRunning.value = True

        # first bind and listen to the port
        self.serverTransport.listen()

        # fork the children
        semaphore = Semaphore(0)
        for _ in range(self.numWorkers):
            try:
                w = Process(target=self.workerProcess, args=(semaphore,))
                w.daemon = True
                w.start()
                self.workers.append(w)
            except Exception, x:
                logger.exception(x)

        # wait until all workers init finish
        for _ in range(self.numWorkers):
            semaphore.acquire()

        # wait until the condition is set by stop()
        while True:
            try:
                gevent.sleep(1)
                if not self.isRunning.value:
                    break
            except (SystemExit, KeyboardInterrupt):
                break
            except Exception, x:
                logger.exception(x)

        self.isRunning.value = False

    def stop(self):
        self.isRunning.value = False

    def setNumCoroutines(self, num):
        self.numCoroutines = num
    