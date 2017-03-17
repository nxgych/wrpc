package com.chen.wrpc.register;

import com.chen.wrpc.server.ServerConfig;

/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public interface ServerRegister {	
	/**
	 * 服务注册
	 * @param path
	 */
	void register(String path); 
	
	/**
	 * 服务注册并监听
	 * @param serverConfig 
	 */
	void registerAndListen(ServerConfig serverConfig);
}
