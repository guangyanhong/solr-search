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
 * ���е�����Ϊ�ļ��ļ����ķַ��������е������ļ��ı�����ᵽ�˶����Ѵ���
 * �˶���ֻ��ͨ�������ļ������ֽ��м򵥵ķַ����ַ�������Ĵ����Watcher������������Ĵ����߼�
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
			log.error("�����ļ������˱��������û���ҵ���Ӧ�������ļ���Watcher����. ==> " + configName + " [" + path+"].");
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
	
	
	//******************************************* �����Ǹ��������ļ��ļ���������  ******************************************//
	
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
	 * solrconfig.xml �����ļ��������
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
			log.warn("solr�����ļ� solrconfig.xml�ļ��������.");
			String path = event.getPath();
			EventType type =event.getType();
			log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t��ȡcoreNameΪ[" + coreName + "]�������ļ� [" + configName + "] ����.",e);
					return ;
				}
				
				try {
					log.warn("����װ����Ϊ[" + coreName +"] ��Core.");
					coreContainer.reload(coreName);
				} catch (Throwable e){
					log.error("\t����װ��core����",e);
					
					log.warn("\t����ת���µ������ļ��������쳣���ʻع���ԭ���Ŀ��õ������ļ�����ɾ����zk�ϵĶ�Ӧ��znode.");
					
					configControllor.rollbackFile(coreName, configName);
					
					try {
						configControllor.rollbackToZk(coreName, configName);
					} catch (Throwable e1) {
						log.error("�ع�ZooKeeper �ϵ�CoreName ==> " + coreName + " confgName ==> " + configName  + "�׳��쳣.",e);
					}
					return;
				}
				
				//����װ��TermiantorService ==> ��solrconfig.xml�ļ�����չ�����ѵķ���
				
			}else if(type == EventType.NodeDeleted){
				log.warn("·��Ϊ[" + path + "]��znode�ڵ㱻ɾ��,���ټ����˽ڵ�.");
			}else{
				log.warn("���¼����� [" + type +"] û����Ӧ�Ĵ���.");
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
			log.warn("solr�����ļ�  ds.xml �ļ��������.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t��ȡcoreNameΪ[" + coreName + "]�������ļ� [" + configName + "] ����,�ع��ļ�..",e);
					configControllor.rollbackFile(coreName, configName);
					return ;
				}
				
				log.warn(" ============>  " + coreName +  " ������Դ�����ļ�ds.xml�Ѿ�˳��д�뱾�����̣���������Dump��ʹ�ô�����Դ����ע��۲��Ƿ�����.");
			}else if(type == EventType.NodeDeleted){
				log.warn("·��Ϊ[" + path + "]��znode�ڵ㱻ɾ��,���ټ����˽ڵ�.");
			}else{
				log.warn("���¼����� [" + type +"] û����Ӧ�Ĵ���.");
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
			log.warn("Terminator����Դ�����ļ�  schema.xml �ļ��������.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t��ȡcoreNameΪ[" + coreName + "]�������ļ� [" + configName + "] ����.",e);
					return ;
				}
				
				try {
					log.warn("����װ����Ϊ[" + coreName +"] ��Core.");
					coreContainer.reload(coreName);
					log.warn("������schema.xml�ļ��ı�����ʿ�����Ҫ����dump���ݣ��Է����µ�schema�Ĺ淶.");
				} catch (Throwable e){
					log.error("\t����װ��core����",e);
					
					log.warn("\t����װ���µ������ļ��������쳣���ʻع���ԭ���Ŀ��õ������ļ������ع���zk�ϵĶ�Ӧ��znode.");
					
					configControllor.rollbackFile(coreName, configName);
					
					try {
						configControllor.rollbackToZk(coreName, configName);
					} catch (Throwable e1) {
						log.error("�ع�ZooKeeper �ϵ�CoreName ==> " + coreName + " confgName ==> " + configName  + "�׳��쳣.",e);
					}
					return;
				}
				
			}else if(type == EventType.NodeDeleted){
				log.warn("·��Ϊ[" + path + "]��znode�ڵ㱻ɾ��,���ټ����˽ڵ�.");
			}else{
				log.warn("���¼����� [" + type +"] û����Ӧ�Ĵ���.");
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
			log.warn("solr�����ļ� applicationContext.xml �ļ��������.");
			String path = event.getPath();
			EventType type =event.getType();
		
			log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				try {
					configControllor.fetchOneConfigFile(coreName, configName);
				} catch (Throwable e){
					log.error("\t��ȡcoreNameΪ[" + coreName + "]�������ļ� [" + configName + "] ����,�ع��ļ�..",e);
					configControllor.rollbackFile(coreName, configName);
					return ;
				}
				
				log.warn(" ============>  " + coreName +  " ��Spring�����ļ�applicationContext.xml�ļ���������ұ����������ļ�ϵͳ����������������ȷ��ApplicationContext���¼���.");
			}else if(type == EventType.NodeDeleted){
				log.warn("·��Ϊ[" + path + "]��znode�ڵ㱻ɾ��,���ټ����˽ڵ�.");
			}else{
				log.warn("���¼����� [" + type +"] û����Ӧ�Ĵ���.");
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
			log.warn("Termiantor��ɫ�����ļ�core.properties�ļ��������.");
			String path = event.getPath();
			EventType type =event.getType();
			log.warn("·��Ϊ[" + path +"]��znode�ڵ㷢�����,EventTypeΪ [" + type+"].");
			
			if(type == EventType.NodeDataChanged){
				byte[] data = null;
				try{
					data = zkClient.getData(path,this);
				}catch(TerminatorZKException e){
					log.error("��ZK��ȡ�µ�core.properties�����ļ�ʧ��",e);
					return;
				} 
				
				String[] rs = analyzePath(path);
				String coreName = rs[0];
				String configName = rs[1];
				
				log.warn("��ȡԭ�е�core.properties�����ļ�.");
			
				InputStream inputStream = null;
				CoreProperties oldPro = null;
				try {
					inputStream = configControllor.readConfigFileFromSolrDir(coreName, configName);
					oldPro = new CoreProperties(inputStream);
				} catch (FileNotFoundException e) {
					log.error("��ȡԭ��core.properties�ļ�����,���Դ˴��µ������ļ�������",e);
					return;
				} catch (IOException e) {
					log.error("����ԭ��core.properties�ļ�����,���Դ˴��µ������ļ�������",e);
					return;
				}
				
				CoreProperties newPro = null;
				try{
					InputStream in = new ByteArrayInputStream(data);
					newPro = new CoreProperties(in);
				} catch (IOException e) {
					log.error("�����µ�core.properties�ļ�����,���Դ˴��µ������ļ�������",e);
					return;
				}
				
				InternalResult result = this.analyzeCoreProperties(oldPro, newPro);
				Set<ServiceType> addServices = result.getNewaddServices();
				Set<ServiceType> delServices = result.getDelServices();
				
				log.warn("HSF������� ==> " + result.toString());
				
				if(delServices != null && delServices.size() > 0){
					log.warn("�賷���Ѿ������ķ��� ==> " + delServices + "����Ŀǰ��ϵͳ��֧�ַ����������Ĺ��ܣ��ʺ��Դ�����.");
				}
				
				if(addServices != null && addServices.size() > 0){
					log.warn("���·���HSF���� ==> " + addServices);
					for(ServiceType as : addServices){
						TerminatorService targetService = MultiServiceContainer.getInstance().getTerminatorService(coreName);
						try {
							TerminatorHSFContainer.publishService(targetService, null, TerminatorHSFContainer.Utils.genearteVersion(as, coreName));
						} catch (TerminatorHsfPubException e) {
							log.error("����HSF����ʧ��.",e);
						}
					}
				}
				
				try {
					log.warn("�����µ�core.properties�ļ���core��confĿ¼.");
					configControllor.writeConfFile2SolrDir(coreName, configName, data);
				} catch (IOException e) {
					log.error("�����µ�core.properties�ļ�ʧ��.");
					return;
				}
				
			}else if(type == EventType.NodeDeleted){
				log.warn("·��Ϊ[" + path + "]��znode�ڵ㱻ɾ��,���ټ����˽ڵ�.");
			}else{
				log.warn("���¼����� [" + type +"] û����Ӧ�Ĵ���.");
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
