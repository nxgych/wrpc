#coding:utf-8

from test.message import MessageService

class MessageHandler(MessageService.Iface):   
    def sendSMS(self, mobile):
        try:
            print mobile
            return True
        except Exception as e:
            raise e  