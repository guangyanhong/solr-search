package com.taobao.terminator.core.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.common.protocol.TerminatorService;

/**
 * 一个机器的多个SolrCore的ServiceContainer   单例模式
 * 
 * @author yusen
 */
public class MultiServiceContainer {
	protected static Log log = LogFactory.getLog(MultiServiceContainer.class);
	
	private CoreContainer coreContainer = null;
	private Map<String,ServiceContainer> serviceContainers = null;
	
	private static MultiServiceContainer multiContainer = null;
	
	/**
	 * 获取MultiServiceContainer对象
	 * 
	 * @return
	 */
	public static MultiServiceContainer getInstance(){
		if(multiContainer == null){
			throw new RuntimeException("MultiServiceContainer对象还没有实例化，请先调用createInstance(CoreContainer)方法.");
		}
		return multiContainer;
	}
	
	/**
	 * 创建该单例对象
	 * 
	 * @param coreContainer
	 */
	public static void createInstance(CoreContainer coreContainer){
		multiContainer = new MultiServiceContainer(coreContainer);
	}
	
	/**
	 * 单例   私有的构造函数
	 * 
	 * @param coreContainer
	 */
	private MultiServiceContainer(CoreContainer coreContainer){
		this.coreContainer = coreContainer;
		this.init();
	}
	
	private void init(){
		serviceContainers = new HashMap<String,ServiceContainer>();
		Collection<SolrCore> solrCores = coreContainer.getCores();
		for(SolrCore solrCore : solrCores){
			ServiceContainer container = new ServiceContainer(solrCore);
			serviceContainers.put(solrCore.getName(), container);
		}
	}
	
	/**
	 * 注册新的ServiceContainer对象
	 * 
	 * @param serviceContainer
	 * @return old ServiceContainer
	 */
	public ServiceContainer registerServiceContainer(ServiceContainer serviceContainer){
		return serviceContainers.put(serviceContainer.getSolrCore().getName(), serviceContainer);
	}
	
	/**
	 * 获取coreName对应的SolrCore的ServiceContainer对象
	 * 
	 * @param coreName
	 * @return
	 */
	public ServiceContainer getServiceContainer(String coreName){
		return this.serviceContainers.get(coreName);
	}
	
	/**
	 * 获取coreName对应的SolrCore的ServiceContainer内的具体名为serviceName的Service对象(Object)
	 * 
	 * @param coreName
	 * @param serviceName
	 * @return
	 */
	public Object getService(String coreName,String serviceName){
		if(serviceContainers.containsKey(coreName)){
			return serviceContainers.get(coreName).getService(serviceName);
		}else{
			log.error("没有名为 [" + coreName +"]的SolrCore对象 .");
			return null;
		}
	}
	
	/**
	 * 获取默认的，系统必须实现的TerminatorService的对象实例
	 * 
	 * @param coreName
	 * @return
	 */
	public TerminatorService getTerminatorService(String coreName){
		if(serviceContainers.containsKey(coreName)){
			return serviceContainers.get(coreName).getTerminatorService();
		}else{
			log.error("没有名为 [" + coreName +"]的SolrCore对象 .");
			return null;
		}
	}
}
