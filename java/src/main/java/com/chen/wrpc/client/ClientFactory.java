package com.chen.wrpc.client;

import java.util.Map;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.chen.wrpc.common.ServerNode;
import com.chen.wrpc.provider.ServerProvider;

/**
 * @author shuai.chen
 * @created 2016年10月31日
 */
public class ClientFactory extends BaseKeyedPooledObjectFactory<String,TServiceClient> {  
	
	private Logger logger = LoggerFactory.getLogger(getClass());
    
	//server provider
    private final ServerProvider serverProvider;  
    //{service name : client class}
    private final  Map<String,TServiceClientFactory<TServiceClient>> clientFactoryMap;  
	
    protected ClientFactory(ServerProvider serverProvider, 
    		 Map<String,TServiceClientFactory<TServiceClient>> clientFactoryMap) throws Exception {  
        this.serverProvider = serverProvider;  
        this.clientFactoryMap = clientFactoryMap;  
    }  
	
    public void destroyObject(final String key, TServiceClient client) throws Exception {  
    		logger.info("Client closed.");
		TTransport pin = client.getInputProtocol().getTransport();
		pin.close();
		TTransport pout = client.getOutputProtocol().getTransport();
		pout.close();
    }  
  
    public boolean validateObject(final String key, TServiceClient client) {  
		TTransport pin = client.getInputProtocol().getTransport();
		TTransport pout = client.getOutputProtocol().getTransport();
		return pin.isOpen() && pout.isOpen();
    }  

	public PooledObject<TServiceClient> wrap(TServiceClient client) {
		// TODO Auto-generated method stub
		return new DefaultPooledObject<TServiceClient>(client);
	}

	@Override
	public TServiceClient create(String key) throws Exception {
		// TODO Auto-generated method stub
        ServerNode address = serverProvider.selector();  
        TSocket tsocket = new TSocket(address.getIp(), address.getPort());  
        TTransport transport = new TFramedTransport(tsocket);  
        TProtocol protocol = new TCompactProtocol(transport);  
        TMultiplexedProtocol mprotocal = new TMultiplexedProtocol(protocol,key);
        TServiceClient client = this.clientFactoryMap.get(key).getClient(mprotocal);  
        transport.open();  
        
        logger.info("Client created."); 
        return client;  
	}

}  