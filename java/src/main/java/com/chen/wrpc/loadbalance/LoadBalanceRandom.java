package com.chen.wrpc.loadbalance;

import java.util.Random;

import com.chen.wrpc.common.ServerNode;
import com.chen.wrpc.common.WrpcException;

/**
 * @author shuai.chen
 * @created 2017年2月28日
 */
public class LoadBalanceRandom extends LoadBalance{

	@Override
	public ServerNode getAddress() {
		// TODO Auto-generated method stub
		int size = nodes.size();
		if (size <= 0){
			throw new WrpcException("Server not found!");
		}
		Random random = new Random();
		int randomPos = random.nextInt(size);
		return nodes.get(randomPos);
	}

}
