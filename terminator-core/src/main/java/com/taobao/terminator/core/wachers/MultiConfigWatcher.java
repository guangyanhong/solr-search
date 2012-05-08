package com.taobao.terminator.core.wachers;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.taobao.terminator.common.CoreProperties;
import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.TerminatorHsfPubException;
import com.taobao.terminator.common.protocol.TerminatorService;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.core.ConfigControllor;
import com.taobao.terminator.core.service.MultiServiceContainer;

/**
 * 所有的配置为文件的监听的分发器，所有的配置文件的变更都会到此对象惊醒处理，
 * 此对象只会通过配置文件的名字进行简单的分发，分发到具体的处理的Watcher，而不做具体的处理逻辑
 * 
 * @author yusen
 */
public class MultiConfigWatcher  extends TerminatorWatcher{
	protected Map<String,ConfigWatcher> configWatherMaps = null;
	
	public MultiConfigWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configPuller) {
		super(zkClient, coreContainer, configPuller);
		this.init();
	}
	
	public MultiConfigWatcher(TerminatorZkClient zkClient){
		super(zkClient);
		this.init();
	}

	private void init() {
		ConfigWatcher solrConfigWatcher = new SolrConfigWatcher(zkClient);
		ConfigWatcher schemaWatcher = new SchemaWatcher(zkClient);
		ConfigWatcher corePropertiesWatcher = new CorePropertiesWatcher(zkClient);
		ConfigWatcher dsWatcher = new DSWatcher(zkClient);
		ConfigWatcher springWatcher = new ApplicationContextWatcher(zkClient);
		
		if(configWatherMaps == null){
			configWatherMaps = new HashMap<String,ConfigWatcher>();
		}
		
		configWatherMaps.put(solrConfigWatcher.getConfigFileName(), solrConfigWatcher);
		configWatherMaps.put(schemaWatcher.getConfigFileName(), schemaWatcher);
		configWatherMaps.put(corePropertiesWatcher.getConfigFileName(), corePropertiesWatcher);
		configWatherMaps.put(dsWatcher.getConfigFileName(), dsWatcher);
		configWatherMaps.put(springWatcher.getConfigFileName(), springWatcher);
	}
	
	@Override
	public void setCoreContainer(CoreContainer coreContainer){
		Set<String> keySet = configWatherMaps.keySet();
		for(String key : keySet){
			configWatherMaps.get(key).setCoreContainer(coreContainer);
		}
	}
	
	public void registConfigWatcher(ConfigWatcher configWatcher){
		if(configWatherMaps == null){
			configWatherMaps = new HashMap<String,ConfigWatcher>();
		}
		configWatherMaps.put(configWatcher.getConfigFileName(), configWatcher);
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if(path == null){
			return;
		}
		String[] rs = analyzePath(path);
		
		String configName = rs[1];
		if(!configWatherMaps.containsKey(configName)){
			log.error("配置文件发生了变更，但是没有找到对应该配置文件的Watcher对象. ==> " + configName + " [" + path+"].");
		}else{
			configWatherMaps.get(configName).process(event);
		}
	}
	
	@Override
	public void setConfigControllor(ConfigControllor configControllor){
		this.configControllor = configControllor;
		Set<String> keySet = configWatherMaps.keySet();
		for(String key : keySet){
			configWatherMaps.get(key).setConfigControllor(configControllor);
		}
	}

	
	protected static String[] analyzePath(String path){
		
		String _path = TerminatorZKUtils.normalizePath(path);
		
		String configName = _path.substring(_path.lastIndexOf(TerminatorZKUtils.SEPARATOR) + 1);
		String p = _path.substring(0,_path.lastIndexOf(TerminatorZKUtils.SEPARATOR));
		String coreName = p.substring(p.lastIndexOf(TerminatorZKUtils.SEPARATOR) + 1);
		
		return new String[]{coreName,configName};
	}
	
	
	//******************************************* 以下是各个配置文件的监听处理类  ******************************************//
	
	public abstract class ConfigWatcher extends TerminatorWatcher{
		public ConfigWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}
		
		public ConfigWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		public abstract String getConfigFileName();
		
	}
	
	
	/**
	 * solrconfig.xml 配置文件变更监听
	 * @author yusen
	 */
	public class SolrConfigWatcher extends ConfigWatcher{

		public SolrConfigWatcher(TerminatorZkClient zkClient, CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}
		
		public SolrConfigWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		@Override
		public void process(WatchedEvent event) {
			log.warn("solr配置文件 solrconfig.xml文件发生变更.");
			String path = event.getPath();
			EventType type =event.getType();
			log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t获取coreName为[" + coreName + "]的配置文件 [" + configName + "] 出错.",e);
					return ;
				}
				
				try {
					log.warn("重新装载名为[" + coreName +"] 的Core.");
					coreContainer.reload(coreName);
				} catch (Throwable e){
					log.error("\t重新装载core出错",e);
					
					log.warn("\t由于转载新的配置文件出现了异常，故回滚到原来的可用的配置文件，并删除在zk上的对应的znode.");
					
					configControllor.rollbackFile(coreName, configName);
					
					try {
						configControllor.rollbackToZk(coreName, configName);
					} catch (Throwable e1) {
						log.error("回滚ZooKeeper 上的CoreName ==> " + coreName + " confgName ==> " + configName  + "抛出异常.",e);
					}
					return;
				}
				
				//重新装载TermiantorService ==> 在solrconfig.xml文件中扩展的终搜的服务
				
			}else if(type == EventType.NodeDeleted){
				log.warn("路径为[" + path + "]的znode节点被删除,不再监听此节点.");
			}else{
				log.warn("对事件类型 [" + type +"] 没有相应的处理.");
			}
		}

		
		@Override
		public String getConfigFileName() {
			return "solrconfig.xml";
		}
	}
	
	public class DSWatcher extends ConfigWatcher {
		
		public DSWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}

		public DSWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		@Override
		public String getConfigFileName() {
			return "ds.xml";
		}

		@Override
		public void process(WatchedEvent event) {
			log.warn("solr配置文件  ds.xml 文件发生变更.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t获取coreName为[" + coreName + "]的配置文件 [" + configName + "] 出错,回滚文件..",e);
					configControllor.rollbackFile(coreName, configName);
					return ;
				}
				
				log.warn(" ============>  " + coreName +  " 的数据源配置文件ds.xml已经顺利写入本机磁盘，接下来的Dump会使用此数据源，请注意观测是否正常.");
			}else if(type == EventType.NodeDeleted){
				log.warn("路径为[" + path + "]的znode节点被删除,不再监听此节点.");
			}else{
				log.warn("对事件类型 [" + type +"] 没有相应的处理.");
			}
		}
	}
	
	
	
	public class SchemaWatcher extends ConfigWatcher{

		public SchemaWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}
		
		public SchemaWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		@Override
		public void process(WatchedEvent event) {
			log.warn("Terminator数据源配置文件  schema.xml 文件发生变更.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t获取coreName为[" + coreName + "]的配置文件 [" + configName + "] 出错.",e);
					return ;
				}
				
				try {
					log.warn("重新装载名为[" + coreName +"] 的Core.");
					coreContainer.reload(coreName);
					log.warn("由于是schema.xml文件的变更，故可能需要重新dump数据，以符合新的schema的规范.");
				} catch (Throwable e){
					log.error("\t重新装载core出错",e);
					
					log.warn("\t由于装载新的配置文件出现了异常，故回滚到原来的可用的配置文件，并回滚在zk上的对应的znode.");
					
					configControllor.rollbackFile(coreName, configName);
					
					try {
						configControllor.rollbackToZk(coreName, configName);
					} catch (Throwable e1) {
						log.error("回滚ZooKeeper 上的CoreName ==> " + coreName + " confgName ==> " + configName  + "抛出异常.",e);
					}
					return;
				}
				
			}else if(type == EventType.NodeDeleted){
				log.warn("路径为[" + path + "]的znode节点被删除,不再监听此节点.");
			}else{
				log.warn("对事件类型 [" + type +"] 没有相应的处理.");
			}
		
		}

		@Override
		public String getConfigFileName() {
			return "schema.xml";
		}
	}
	
	public class ApplicationContextWatcher extends ConfigWatcher {
		
		public ApplicationContextWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}
		
		public ApplicationContextWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		@Override
		public String getConfigFileName() {
			return "applicationContext.xml";
		}

		@Override
		public void process(WatchedEvent event) {
			log.warn("solr配置文件 applicationContext.xml 文件发生变更.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t获取coreName为[" + coreName + "]的配置文件 [" + configName + "] 出错,回滚文件..",e);
					configControllor.rollbackFile(coreName, configName);
					return ;
				}
				
				log.warn(" ============>  " + coreName +  " 的Spring配置文件applicationContext.xml文件变更，并且保存至本机文件系统，请重启服务器，确保ApplicationContext重新加载.");
			}else if(type == EventType.NodeDeleted){
				log.warn("路径为[" + path + "]的znode节点被删除,不再监听此节点.");
			}else{
				log.warn("对事件类型 [" + type +"] 没有相应的处理.");
			}
		}
	}
	
	public class CorePropertiesWatcher extends ConfigWatcher{
		
		public CorePropertiesWatcher(TerminatorZkClient zkClient,CoreContainer coreContainer, ConfigControllor configControllor) {
			super(zkClient, coreContainer, configControllor);
		}
		
		public CorePropertiesWatcher(TerminatorZkClient zkClient) {
			super(zkClient);
		}

		@Override
		public void process(WatchedEvent event) {
			log.warn("Termiantor角色配置文件core.properties文件发生变更.");
			String path = event.getPath();
			EventType type =event.getType();
			log.warn("路径为[" + path +"]的znode节点发生变更,EventType为 [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				byte[] data = null;
				try{
					data = zkClient.getData(path,this);
				}catch(TerminatorZKException e){
					log.error("从ZK获取新的core.properties配置文件失败",e);
					return;
				} 
				
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				log.warn("读取原有的core.properties属性文件.");
			
				InputStream inputStream = null;
				CoreProperties oldPro = null;
				try {
					inputStream = configControllor.readConfigFileFromSolrDir(coreName, configName);
					oldPro = new CoreProperties(inputStream);
				} catch (FileNotFoundException e) {
					log.error("读取原有core.properties文件出错,忽略此次新的配置文件的推送",e);
					return;
				} catch (IOException e) {
					log.error("加载原有core.properties文件出错,忽略此次新的配置文件的推送",e);
					return;
				}
				
				CoreProperties newPro = null;
				try{
					InputStream in = new ByteArrayInputStream(data);
					newPro = new CoreProperties(in);
				} catch (IOException e) {
					log.error("加载新的core.properties文件出错,忽略此次新的配置文件的推送",e);
					return;
				}
				
				InternalResult result = this.analyzeCoreProperties(oldPro, newPro);
				Set<ServiceType> addServices = result.getNewaddServices();
				Set<ServiceType> delServices = result.getDelServices();
				
				log.warn("HSF服务调整 ==> " + result.toString());
				
				if(delServices != null && delServices.size() > 0){
					log.warn("需撤销已经发布的服务 ==> " + delServices + "但是目前此系统不支持服务撤销发布的功能，故忽略此请求.");
				}
				
				if(addServices != null && addServices.size() > 0){
					log.warn("需新发布HSF服务 ==> " + addServices);
					for(ServiceType as : addServices){
						TerminatorService targetService = MultiServiceContainer.getInstance().getTerminatorService(coreName);
						try {
							TerminatorHSFContainer.publishService(targetService, null, TerminatorHSFContainer.Utils.genearteVersion(as, coreName));
						} catch (TerminatorHsfPubException e) {
							log.error("发布HSF服务失败.",e);
						}
					}
				}
				
				try {
					log.warn("保存新的core.properties文件到core的conf目录.");
					configControllor.writeConfFile2SolrDir(coreName, configName, data);
				} catch (IOException e) {
					log.error("保存新的core.properties文件失败.");
					return;
				}
				
			}else if(type == EventType.NodeDeleted){
				log.warn("路径为[" + path + "]的znode节点被删除,不再监听此节点.");
			}else{
				log.warn("对事件类型 [" + type +"] 没有相应的处理.");
			}
		}
		
		private InternalResult analyzeCoreProperties(CoreProperties oldPro,CoreProperties newPro){
			boolean oldIsMerger = oldPro.isMerger();
			boolean oldIsReader = oldPro.isReader();
			boolean oldIsWriter = oldPro.isWriter();
			
			boolean newIsMerger = newPro.isMerger();
			boolean newIsReader = newPro.isReader();
			boolean newIsWriter = newPro.isWriter();
			
			InternalResult rs = new InternalResult();
			
			if(!oldIsMerger && newIsMerger){
				rs.addNewaddService(ServiceType.merger);
			}
			
			if(!oldIsReader && newIsReader){
				rs.addNewaddService(ServiceType.reader);
			}
			
			if(!oldIsWriter && newIsWriter){
				rs.addNewaddService(ServiceType.writer);
			}
			
			if(oldIsMerger && !newIsMerger){
				rs.addDelService(ServiceType.merger);
			}
			
			if(oldIsReader && !newIsReader){
				rs.addDelService(ServiceType.reader);
			}
			
			if(oldIsWriter && !newIsWriter){
				rs.addDelService(ServiceType.writer);
			}
			
			return rs;
		}
		
		private class InternalResult{
			Set<ServiceType> addServices = new HashSet<ServiceType>();
			Set<ServiceType> delServices = new HashSet<ServiceType>();
			
			void addNewaddService(ServiceType type){
				addServices.add(type);
			}
			
			void addDelService(ServiceType type){
				delServices.add(type);
			}
			
			Set<ServiceType> getNewaddServices(){
				return addServices;
			}
			
			Set<ServiceType> getDelServices(){
				return delServices;
			}
			
			public String toString(){
				StringBuilder sb = new StringBuilder();
				sb.append("need to add service is [");
				for(ServiceType  as : addServices){
					sb.append(as);
				}
				sb.append("\t");
				sb.append("need to unregister service is [");
				for(ServiceType  ds : delServices){
					sb.append(ds);
				}
				
				return sb.toString();
			}
		}

		@Override
		public String getConfigFileName() {
			return "core.properties";
		}
	}
}
