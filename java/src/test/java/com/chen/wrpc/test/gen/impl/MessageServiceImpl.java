package com.chen.wrpc.test.gen.impl;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chen.wrpc.test.gen.MessageService;
/**
 * @author shuai.chen
 * @created 2017年2月28日
 */
public class MessageServiceImpl implements MessageService.Iface{

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	public boolean sendSMS(String mobile) throws TException {
		// TODO Auto-generated method stub
		logger.info(mobile);
		System.out.println(mobile);
		return true;
	}

}
