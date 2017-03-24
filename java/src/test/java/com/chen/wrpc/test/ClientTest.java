package com.chen.wrpc.test;

import com.chen.wrpc.client.ClientProxy;
import com.chen.wrpc.client.ZookeeperFactory;
import com.chen.wrpc.provider.ServerProviderAuto;

import com.chen.wrpc.test.gen.MessageService;

/**
 * @author shuai.chen
 * @created 2017年3月16日
 */
public class ClientTest {

	/**
	 * 建议使用 spring 配置
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 
		 //zookeeper 客户端
		 @SuppressWarnings("resource")
		 ZookeeperFactory zkClient = new ZookeeperFactory();
		 zkClient.setZkHosts("127.0.0.1:2181");
		 zkClient.setSessionTimeout(5000);
		 
		 //server provider
		 ServerProviderAuto serverProvider = new ServerProviderAuto();
		 serverProvider.setZkClient(zkClient.getObject());
		 serverProvider.setGlobalService("com.wrpc.test");
		 serverProvider.setServiceInterfaces(new String[]{"com.chen.wrpc.test.gen.MessageService"});
		 serverProvider.afterPropertiesSet();
		 
		 //客户端
		 @SuppressWarnings("resource")
		 ClientProxy client = new ClientProxy();
		 client.setServerProvider(serverProvider);
		 client.afterPropertiesSet();

		 //接口调用
		 MessageService.Iface ms = (MessageService.Iface)client.getClient(MessageService.class);
		 ms.sendSMS("110");
	}

}
