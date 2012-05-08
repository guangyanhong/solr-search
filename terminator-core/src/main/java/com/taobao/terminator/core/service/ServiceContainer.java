package com.taobao.terminator.core.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.common.protocol.TerminatorService;

/**
 * 利用Solr的PluginInfo,在solrconfig.xml配置文件中配置自己的TerminatorService,让solrcore自行加载
 * 一个ServiceContainer代表一个SolrCore内的终搜实现的自己的Service,默认必须配置的则是TerminatorService
 * 
 * @author yusen
 */
public class ServiceContainer {
	protected static Log log = LogFactory.getLog(ServiceContainer.class);
	
	private static final String xmlTag = "terminatorService";
	private static final String terminatorServiceName = "terminatorService";
	private static String defaultClassName = DefaultTerminatorService.class.getName();

	private SolrCore solrCore = null;
	private Map<String, Object> services = null;
	
	public ServiceContainer(SolrCore solrCore) {
		this.solrCore = solrCore;
		this.init();
	}

	public void init(){
		services = new HashMap<String,Object>();
		log.warn("加载TerminatorService对象.");
		List<PluginInfo> pluginInfos = solrCore.getSolrConfig().readPluginInfos(xmlTag, true, true);
		boolean containsRequiredService = false;
		for(PluginInfo pluginInfo : pluginInfos){
			log.warn("\t 加载TermiantorService对象 ==> " + pluginInfo);
			Object obj = solrCore.createInitInstance(pluginInfo, null, "Terminator Service", defaultClassName);
			services.put(pluginInfo.name, obj);
			if(pluginInfo.name.equals(terminatorServiceName)){
				containsRequiredService = true;
			}
		}
		
		if(!containsRequiredService){
			log.error("solrconfig.xml文件中不包含名为 [" + terminatorServiceName + "] 的terminatorService节点.");
			throw new RuntimeException("solrconfig.xml文件中不包含名为 [" + terminatorServiceName + "] 的terminatorService节点.");
		}
	}
	
	public void reload(){
		//TODO  当solrconfig.xml配置文件中关于<terminatorService> 节点发生变更（增、减）时候的动作
	}
	
	public synchronized void unregisterService(String serviceName){
		log.warn("注销TerminatorService服务  solrCore ==>" + solrCore.getName() + "serviceName ==> " + serviceName);
		Object obj = services.remove(serviceName);
		if(obj != null && obj instanceof Lifecycle){
			((Lifecycle)obj).stop();
			obj = null;
		}
	}
	
	public synchronized void unregisterAllServices(){
		log.warn("注销所有TerminatorService服务  solrCore ==>" + solrCore.getName());
		Set<String> serviceNames = services.keySet();
		for(String serviceName : serviceNames){
			Object obj = services.get(serviceName);
			if(obj != null && obj instanceof Lifecycle){
				((Lifecycle)obj).stop();
				obj = null;
			}
		}
		services.clear();
	}
	
	public Object getService(String name){
		return services.get(name);
	}
	
	public TerminatorService getTerminatorService(){
		return (TerminatorService)services.get(terminatorServiceName);
	}

	public SolrCore getSolrCore() {
		return solrCore;
	}

	public void setSolrCore(SolrCore solrCore) {
		this.solrCore = solrCore;
	}

	public Map<String, Object> getServices() {
		return services;
	}

	public void setServices(Map<String, Object> services) {
		this.services = services;
	}
}
