package com.chen.wrpc.server;

import java.io.Closeable;
import java.lang.reflect.Constructor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.chen.wrpc.common.WrpcException;
import com.chen.wrpc.register.ServerRegister;
import com.chen.wrpc.register.ServerRegisterImpl;

/**
 * @author shuai.chen
 * @created 2017年2月22日
 */
public class Server implements InitializingBean,Closeable{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	//Zookeeper 客户端
	private CuratorFramework zkClient;
	//服务配置
	private ServerConfig serverConfig;
    //server thread
	private ServerThread serverThread;
	//server register object
	private ServerRegister serverRegister;
	
	public Server(){}
		
	public void setZkClient(CuratorFramework zkClient){
		this.zkClient = zkClient;
	}
	
	public void setServerConfig(ServerConfig serverConfig){
		this.serverConfig = serverConfig;
	}
	
	public void setServerThread(ServerThread serverThread){
		this.serverThread = serverThread;
	} 
	
	/**
	 * set server thread 
	 */
	public void setServerThread(){
    		try{
	        TMultiplexedProcessor processor = new TMultiplexedProcessor();

	        Object[] serviceImpls = serverConfig.getServiceProcessors();
	        for(int i=0; i<serviceImpls.length; i++){
		        	Class<?> serviceClass = serviceImpls[i].getClass();
		        	
		        	String serviceName = null;
		        	Class<?>[] interfaces = serviceClass.getInterfaces();
		        	for (Class<?> clazz : interfaces) {
		    			String cname = clazz.getSimpleName();
		    			if (!cname.equals("Iface")) {
		    				continue;
		    			}	
		    			
		    			serviceName = clazz.getEnclosingClass().getName();
		    			Class<?> sProcessor = Class.forName(serviceName + "$Processor"); 
		    			Constructor<?> con = sProcessor.getConstructor(clazz); 
	
		    			String serviceSimpleName = clazz.getEnclosingClass().getSimpleName();
		    			TProcessor tProcessor = (TProcessor)con.newInstance(serviceImpls[i]);
						processor.registerProcessor(serviceSimpleName, tProcessor);    
						break;	    			
		         }               
	        }   
	        
	        this.serverThread = new ServerThread(processor);	   	        
	    	}catch(Exception e){
	    		throw new WrpcException(e); 
	    	}		
	}
	
	public void setServerRegister(ServerRegister serverRegister){
		this.serverRegister = serverRegister;
	}
	
	public void setServerRegister(){
		if(zkClient != null) {
			this.serverRegister = new ServerRegisterImpl(zkClient);
		}
	}
	
	/**
	 * 服务注册
	 */
	public void register(){
		if(serverConfig != null && serverRegister != null){
			serverRegister.registerAndListen(serverConfig);  
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
	    this.setServerThread();  
	    this.setServerRegister();
	}  

    public void start() {  
      	register();
	    	if(serverThread != null){
	    		serverThread.start();  
	    	}
    }

    public void close() {  
	    	if(serverThread != null){
	    		serverThread.stopServer();  
	    	}
	    	if(zkClient != null){
	    		zkClient.close();
	    	}
    } 
    
    /**
     * server thread class
     * 默认使用了非阻塞、压缩的传输方式
     */
	class ServerThread extends Thread {  
		
        private TServer server;  
        
		ServerThread(TProcessor processor) throws Exception {  
			TNonblockingServerSocket transport = new TNonblockingServerSocket(serverConfig.getPort());  	
            TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport);   
	        	args.transportFactory(new TFramedTransport.Factory());    
	        	args.protocolFactory(new TCompactProtocol.Factory());  
	        	args.processor(processor);                
            server = new TThreadedSelectorServer(args);  
        }  
  
        @Override  
        public void run(){  
            try{  
                //启动服务  
                server.serve();  
                logger.info("thrift server started!");
            }catch(Exception e){  
                throw new WrpcException(e); 
            }  
        }  
          
        public void stopServer(){  
            server.stop();  
            logger.info("thrift server stopped!");
        }  
    }

}
