package com.chen.wrpc.loadbalance;

import com.chen.wrpc.common.ServerNode;
import com.chen.wrpc.common.WrpcException;

/**
 * @author shuai.chen
 * @created 2017年2月28日
 */
public class LoadBalanceRoundRobin extends LoadBalance{
	
	private static Integer pos = 0;
	
	@Override
	public ServerNode getAddress() {
		// TODO Auto-generated method stub
		ServerNode server = null;
		int size = nodes.size();
		if (size <= 0){
			throw new WrpcException("Server not found!");
		}
		synchronized (pos)
		{
			if (pos >= size){
				pos = 0;
			}
			server = nodes.get(pos);
			pos ++;
		}
		return server;
	}

}
