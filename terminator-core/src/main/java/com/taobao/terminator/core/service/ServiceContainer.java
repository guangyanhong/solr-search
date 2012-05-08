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
 * ����Solr��PluginInfo,��solrconfig.xml�����ļ��������Լ���TerminatorService,��solrcore���м���
 * һ��ServiceContainer����һ��SolrCore�ڵ�����ʵ�ֵ��Լ���Service,Ĭ�ϱ������õ�����TerminatorService
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
		log.warn("����TerminatorService����.");
		List<PluginInfo> pluginInfos = solrCore.getSolrConfig().readPluginInfos(xmlTag, true, true);
		boolean containsRequiredService = false;
		for(PluginInfo pluginInfo : pluginInfos){
			log.warn("\t ����TermiantorService���� ==> " + pluginInfo);
			Object obj = solrCore.createInitInstance(pluginInfo, null, "Terminator Service", defaultClassName);
			services.put(pluginInfo.name, obj);
			if(pluginInfo.name.equals(terminatorServiceName)){
				containsRequiredService = true;
			}
		}
		
		if(!containsRequiredService){
			log.error("solrconfig.xml�ļ��в�������Ϊ [" + terminatorServiceName + "] ��terminatorService�ڵ�.");
			throw new RuntimeException("solrconfig.xml�ļ��в�������Ϊ [" + terminatorServiceName + "] ��terminatorService�ڵ�.");
		}
	}
	
	public void reload(){
		//TODO  ��solrconfig.xml�����ļ��й���<terminatorService> �ڵ㷢���������������ʱ��Ķ���
	}
	
	public synchronized void unregisterService(String serviceName){
		log.warn("ע��TerminatorService����  solrCore ==>" + solrCore.getName() + "serviceName ==> " + serviceName);
		Object obj = services.remove(serviceName);
		if(obj != null && obj instanceof Lifecycle){
			((Lifecycle)obj).stop();
			obj = null;
		}
	}
	
	public synchronized void unregisterAllServices(){
		log.warn("ע������TerminatorService����  solrCore ==>" + solrCore.getName());
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
