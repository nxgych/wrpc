package com.chen.wrpc.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * 自定义context
 * @author shuai.chen
 * @created 2016年11月15日
 */
public class SpringContext implements ApplicationContextAware{

	 // Spring应用上下文环境  
    private static ApplicationContext applicationContext;  
    
    /** 
     * 实现ApplicationContextAware接口的回调方法，设置上下文环境 
     *  
     * @param applicationContext 
     */  
    public void setApplicationContext(ApplicationContext applicationContext) {  
        SpringContext.applicationContext = applicationContext;  
    }  
    
    /** 
     * @return ApplicationContext 
     */  
    public static ApplicationContext getApplicationContext() {  
        return applicationContext;  
    }  
    
    /** 
     * 获取对象 
     * @param name 
     * @return Object
     * @throws BeansException 
     */  
    public static Object getBean(String name) throws BeansException {  
        return applicationContext.getBean(name);  
    }  

    /**
     * 获取对象 
     * @param clazz
     * @return
     * @throws BeansException
     */
    public static Object getBean(Class<?> clazz) throws BeansException {  
        return applicationContext.getBean(clazz);  
    }  
    
}
