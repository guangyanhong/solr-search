package com.taobao.terminator.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;

import com.taobao.terminator.client.router.GroupRouter;
import com.taobao.terminator.client.router.ServiceConfigAware;
import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.TerminatorHsfSubException;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.config.GroupConfig;
import com.taobao.terminator.common.config.HostConfig;
import com.taobao.terminator.common.config.HostStatusHolder;
import com.taobao.terminator.common.config.ServiceConfig;
import com.taobao.terminator.common.config.ServiceConfigSupport;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.TerminatorService;
import com.taobao.terminator.common.zk.TerminatorZkClient;


/**
 * �ݿͻ���Bean��Dump�߼���Server�˵Ŀͻ����ô�Bean
 * 
 * @author yusen
 */
public class ThinTerminatorBean implements ServiceConfigSupport{
	protected static Log logger = LogFactory.getLog(TerminatorBean.class);
	
	public static final int DEFAULT_ZK_TIMEOUT         = 300000;
	public static final String DEFAULT_SERVLET_CONTEXT = "terminator-search";
	
	protected int       		   zkTimeout          = DEFAULT_ZK_TIMEOUT;
	protected String    		   servletContextName = DEFAULT_SERVLET_CONTEXT;
	protected String   		 	   zkAddress;
	protected String               serviceName;
	
	protected ServiceConfig        serviceConfig;
	protected HostStatusHolder     hostStatusHolder;
	protected TerminatorZkClient   zkClient;  
	protected boolean              canConnectToZK;
	protected GroupRouter          groupRouter;

	public void init() throws TerminatorInitException{
		this.checkFields();
		this.initServiceConfig();
		this.initGroupRouter();
		this.subscribeHSFService();
		this.initHostStatusHolder();
	}
	
	private void initGroupRouter()  throws TerminatorInitException{
		if(groupRouter == null){
			groupRouter = new GroupRouter() {
				@Override
				public String getGroupName(Map<String, String> rowData) {
					return TerminatorConstant.SINGLE_CORE_GROUP_NAME;
				}
			};
		}
		if(groupRouter != null && groupRouter instanceof ServiceConfigAware){
			((ServiceConfigAware)groupRouter).setServiceConfig(serviceConfig);
		}
	}
	
	private void checkFields() throws TerminatorInitException{
		if(StringUtils.isBlank(serviceName)){
			throw new TerminatorInitException("[��Ա������֤ ] serviceName����Ϊ��.");
		}
		if(StringUtils.isBlank(zkAddress)){
			throw new TerminatorInitException("[��Ա������֤ ] zkAddress����Ϊ��.");
		}
	}
	
	private void initServiceConfig() throws TerminatorInitException{
		try {
			this.zkClient = new TerminatorZkClient(zkAddress,zkTimeout,null,true);
			this.canConnectToZK = true;
		} catch (Exception e) {
			logger.error("������������ZooKeeper.",e);
		}
		
		if(this.canConnectToZK){
			logger.warn("�ͻ�������������ZooKeeper��Server�ˣ��ʴ�ZK�ϼ�����Ӧ��ServiceConfig����.");
			try {
				this.serviceConfig = new ServiceConfig(serviceName,zkClient,this);
				this.serviceConfig.checkBySelf();
			} catch (Exception e) {
				throw new TerminatorInitException("��ʼ��TerminatorBean�쳣 ==> ��ʼ��ServiceConfig�����쳣 [FROM-ZK]",e);
			}
			
			if(this.serviceConfig != null){
				try {
					ServiceConfig.backUp2LocalFS(this.serviceConfig);
				} catch (IOException e) {
					throw new TerminatorInitException("����ZooKeeper��װ�ص�ServiceConfig����־û��������ļ�ϵͳ��Ϊ����ʧ��!",e);
				}
			}
		}else{
			logger.warn("�ͻ��˲�����������ZooKeeper��Server�ˣ��ʴӱ����ļ�ϵͳ�����ϴ��������ݵ�ServiceConfig����.");
			try {
				this.serviceConfig = ServiceConfig.loadFromLocalFS();
				this.serviceConfig.checkBySelf();
			} catch (Exception e) {
				throw new TerminatorInitException("��ʼ��TerminatorBean�쳣 ==> ��ʼ��ServiceConfig�����쳣 [FROM-LOCAL-FS]",e);
			}
		}
	}
	
	protected void subscribeHSFService()throws TerminatorInitException{
		this.onServiceConfigChange(serviceConfig);
	}
	
	private void initHostStatusHolder(){
		if(canConnectToZK){
			hostStatusHolder = new HostStatusHolder(zkClient, serviceConfig);
		}else{ //ZooKeeper�����õ����
			hostStatusHolder = new HostStatusHolder(zkClient, serviceConfig){
				private static final long serialVersionUID = -7034391383912579505L;
				public void initState(){}
				public boolean isAlive(String ip){
					return true;
				}
			};
		}
	}
	
	public QueryResponse query(TerminatorQueryRequest query) throws TerminatorServiceException{
		if(isSingleGroup()){
			if(logger.isDebugEnabled()){
				logger.debug("û�з��������.");
			}
			String coreName = serviceConfig.getSingleCoreName();
			if(logger.isDebugEnabled()){
				logger.debug("Search-Content ==> " + query.getQuery());
			}
			return this.getTerminatorService(ServiceType.reader, coreName).query(query);
		}else{
			if(logger.isDebugEnabled()){
				logger.debug("�з��������.");
			}
			Set<String> groupNameSet = null;
			
			//��������shard
			if(query.containsRouteValues() && groupRouter != null){
				if(logger.isDebugEnabled()){
					logger.debug("Query�������Route��Ϣ������������Route���󣬹ʸ���Router�������·������.");
				}
				groupNameSet = new HashSet<String>();
				List<Map<String,String>> routeValues = query.getRouteValues();
				
				for(Map<String,String> routeValue : routeValues){
					String groupName = null;
					try{
						groupName = groupRouter.getGroupName(routeValue);
					}catch(Throwable e){
						logger.warn("GroupRouter.getGroupName(routeValue)��������.",e);
						continue;
					}
					groupNameSet.add(groupName);
				}
			}else{ //�������е�shard
				if(logger.isDebugEnabled()){
					logger.debug("Query���󲻰���·����Ϣ�����������е�Shard.");
				}
				groupNameSet = serviceConfig.keySet();
			}
			
			//��ֱ�Ӷ�λ��һ��shard
			if(groupNameSet.size() == 1){
				String groupName = groupNameSet.iterator().next();
				String coreName = serviceName  + TerminatorConstant.CORENAME_SEPERATOR + groupName;
				if(logger.isDebugEnabled()){
					logger.debug("��ֱ�Ӷ�λ��һ������shard������ ==> coreName :" + coreName);
				}
				return this.getTerminatorService(ServiceType.reader, coreName).query(query);
			}
			
			//��Ҫ�������shard
			StringBuilder shardurlSb = new StringBuilder();
			for(String groupName : groupNameSet){
				GroupConfig groupConfig = serviceConfig.getGroupConfig(groupName);
				
				//�ҳ�����������IP�н�ɫΪreader�����п��õĻ�����IP
				Set<String> ipSet = groupConfig.keySet();
				for(String ip : ipSet){
					HostConfig hostConfig = groupConfig.getHostConfig(ip);
					if(!hostConfig.isReader()){ //�޳���ɫ����reader
						ipSet.remove(ip);
					}
				}
				List<String> options = new ArrayList<String>(ipSet.size());
				for(String ip : ipSet){
					if(hostStatusHolder.isAlive(ip)){
						options.add(ip);
					}
				}
				
				if(options.isEmpty()) {
					logger.error("GroupNameΪ " + groupName +" �ķ������ȫ����û��û�з���Ϊreader�ķ��񣬹ʲ��������͸��������.");
					continue;
				}
				
				//���ѡ��һ�����õ�IP
				String  targetIp = options.get(new Random().nextInt(options.size()));
				String  port     = groupConfig.getHostConfig(targetIp).getPort();
				
				StringBuilder sb = new StringBuilder();
				sb.append(targetIp).append(":").append(port).append("/").append(servletContextName).append("/").append(serviceName).append(TerminatorConstant.CORENAME_SEPERATOR).append(groupName);
				shardurlSb.append(sb.toString()).append(",");
			}
			
			String _shardurl = shardurlSb.toString();
			String shardurl = _shardurl.substring(0,_shardurl.length()-1);
			
			if(logger.isDebugEnabled()){
				logger.debug("�˴������Ƕ�shard����������shard-string Ϊ ==> " + shardurl);
			}
			query.add(ShardParams.SHARDS,shardurl);
			
			return this.getTerminatorService(ServiceType.merger, serviceName).query(query);
		}
	}
	
	protected TerminatorService getTerminatorService(ServiceType serviceType,String coreName){
		return null;
		//return TerminatorHSFContainer.getTerminatorService(serviceType, coreName);
	}
	
	private boolean isSingleGroup(){
		return serviceConfig.isSingle();
	}

	public String getZkAddress() {
		return zkAddress;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	public int getZkTimeout() {
		return zkTimeout;
	}

	public void setZkTimeout(int zkTimeout) {
		this.zkTimeout = zkTimeout;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public ServiceConfig getServiceConfig() {
		return serviceConfig;
	}

	public void onServiceConfigChange(ServiceConfig serviceConfig) {
		this.serviceConfig = serviceConfig;
		Set<String> versionSet = TerminatorHSFContainer.Utils.generateTerminatorVersions(this.serviceConfig.getCoreNameSet());
		for(String version : versionSet){
			logger.warn("(����)����HSF���� ===> " + version);
			try {
                //TODO
				//TerminatorHSFContainer.subscribeService(TerminatorService.class.getName(), version);
			} catch (Exception e) {
				logger.error("����HSF�����쳣 ==> " + version);
				throw new TerminatorInitException(e);
			}
		}
	}

	public GroupRouter getGroupRouter() {
		return groupRouter;
	}

	public void setGroupRouter(GroupRouter groupRouter) {
		this.groupRouter = groupRouter;
	}

	public String getServletContextName() {
		return servletContextName;
	}

	public void setServletContextName(String servletContextName) {
		this.servletContextName = servletContextName;
	}

	public HostStatusHolder getHostStatusHolder() {
		return hostStatusHolder;
	}

	public void setHostStatusHolder(HostStatusHolder hostStatusHolder) {
		this.hostStatusHolder = hostStatusHolder;
	}

	public boolean isCanConnectToZK() {
		return canConnectToZK;
	}

}
