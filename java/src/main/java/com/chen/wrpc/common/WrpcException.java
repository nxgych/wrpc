package com.chen.wrpc.common;

/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public class WrpcException extends RuntimeException{
	
	private static final long serialVersionUID = 1L;

	public WrpcException(){
		super();
	}
	
	public WrpcException(String msg){
		super(msg);
	}
	
	public WrpcException(Throwable e){
		super(e);
	}
	
	public WrpcException(String msg,Throwable e){
		super(msg,e);
	}
}
