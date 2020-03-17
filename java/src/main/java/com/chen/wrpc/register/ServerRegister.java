package com.chen.wrpc.register;


/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public interface ServerRegister {	
	/**
	 * 服务注册
	 */
	void register(); 
	
	/**
	 * 服务注册并监听
	 */
	void registerAndListen();
}
