package com.chen.wrpc.test;

import com.chen.wrpc.zk.ZookeeperFactory;
import com.chen.wrpc.server.Server;
import com.chen.wrpc.server.ServerConfig;
import com.chen.wrpc.test.gen.impl.MessageServiceImpl;

/**
 * @author shuai.chen
 * @created 2017年3月16日
 */
public class ServerTest {

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
		
		 //服务配置
         ServerConfig serverConfig = new ServerConfig();
         serverConfig.setGlobalServiceName("com.wrpc.test");
         serverConfig.setServiceProcessors(new Object[]{new MessageServiceImpl()});
         serverConfig.afterPropertiesSet();
         
         //服务类
         @SuppressWarnings("resource")
		 Server server = new Server();
         server.setZkClient(zkClient.getObject());
         server.setServerConfig(serverConfig);
         server.afterPropertiesSet();  

         //服务启动
         server.start();
	}

}
