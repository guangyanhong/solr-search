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
 * 瘦客户端Bean，Dump逻辑在Server端的客户端用此Bean
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
			throw new TerminatorInitException("[成员属性验证 ] serviceName不能为空.");
		}
		if(StringUtils.isBlank(zkAddress)){
			throw new TerminatorInitException("[成员属性验证 ] zkAddress不能为空.");
		}
	}
	
	private void initServiceConfig() throws TerminatorInitException{
		try {
			this.zkClient = new TerminatorZkClient(zkAddress,zkTimeout,null,true);
			this.canConnectToZK = true;
		} catch (Exception e) {
			logger.error("不能正常连接ZooKeeper.",e);
		}
		
		if(this.canConnectToZK){
			logger.warn("客户端能正常连接ZooKeeper的Server端，故从ZK上加载相应的ServiceConfig对象.");
			try {
				this.serviceConfig = new ServiceConfig(serviceName,zkClient,this);
				this.serviceConfig.checkBySelf();
			} catch (Exception e) {
				throw new TerminatorInitException("初始化TerminatorBean异常 ==> 初始化ServiceConfig对象异常 [FROM-ZK]",e);
			}
			
			if(this.serviceConfig != null){
				try {
					ServiceConfig.backUp2LocalFS(this.serviceConfig);
				} catch (IOException e) {
					throw new TerminatorInitException("将从ZooKeeper上装载的ServiceConfig对象持久化到本地文件系统作为备份失败!",e);
				}
			}
		}else{
			logger.warn("客户端不能正常连接ZooKeeper的Server端，故从本机文件系统加载上次启动备份的ServiceConfig对象.");
			try {
				this.serviceConfig = ServiceConfig.loadFromLocalFS();
				this.serviceConfig.checkBySelf();
			} catch (Exception e) {
				throw new TerminatorInitException("初始化TerminatorBean异常 ==> 初始化ServiceConfig对象异常 [FROM-LOCAL-FS]",e);
			}
		}
	}
	
	protected void subscribeHSFService()throws TerminatorInitException{
		this.onServiceConfigChange(serviceConfig);
	}
	
	private void initHostStatusHolder(){
		if(canConnectToZK){
			hostStatusHolder = new HostStatusHolder(zkClient, serviceConfig);
		}else{ //ZooKeeper不可用的情况
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
				logger.debug("没有分组的搜索.");
			}
			String coreName = serviceConfig.getSingleCoreName();
			if(logger.isDebugEnabled()){
				logger.debug("Search-Content ==> " + query.getQuery());
			}
			return this.getTerminatorService(ServiceType.reader, coreName).query(query);
		}else{
			if(logger.isDebugEnabled()){
				logger.debug("有分组的搜索.");
			}
			Set<String> groupNameSet = null;
			
			//搜索部分shard
			if(query.containsRouteValues() && groupRouter != null){
				if(logger.isDebugEnabled()){
					logger.debug("Query对象包含Route信息，并且配置了Route对象，故根据Router对象进行路由搜索.");
				}
				groupNameSet = new HashSet<String>();
				List<Map<String,String>> routeValues = query.getRouteValues();
				
				for(Map<String,String> routeValue : routeValues){
					String groupName = null;
					try{
						groupName = groupRouter.getGroupName(routeValue);
					}catch(Throwable e){
						logger.warn("GroupRouter.getGroupName(routeValue)方法出错.",e);
						continue;
					}
					groupNameSet.add(groupName);
				}
			}else{ //搜索所有的shard
				if(logger.isDebugEnabled()){
					logger.debug("Query对象不包含路由信息，故搜索所有的Shard.");
				}
				groupNameSet = serviceConfig.keySet();
			}
			
			//能直接定位到一个shard
			if(groupNameSet.size() == 1){
				String groupName = groupNameSet.iterator().next();
				String coreName = serviceName  + TerminatorConstant.CORENAME_SEPERATOR + groupName;
				if(logger.isDebugEnabled()){
					logger.debug("能直接定位到一个具体shard的搜索 ==> coreName :" + coreName);
				}
				return this.getTerminatorService(ServiceType.reader, coreName).query(query);
			}
			
			//需要搜索多个shard
			StringBuilder shardurlSb = new StringBuilder();
			for(String groupName : groupNameSet){
				GroupConfig groupConfig = serviceConfig.getGroupConfig(groupName);
				
				//找出该组中所有IP中角色为reader的所有可用的机器的IP
				Set<String> ipSet = groupConfig.keySet();
				for(String ip : ipSet){
					HostConfig hostConfig = groupConfig.getHostConfig(ip);
					if(!hostConfig.isReader()){ //剔除角色不是reader
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
					logger.error("GroupName为 " + groupName +" 的分组机器全军覆没或没有发布为reader的服务，故不将请求发送给该组机器.");
					continue;
				}
				
				//随机选择一个可用的IP
				String  targetIp = options.get(new Random().nextInt(options.size()));
				String  port     = groupConfig.getHostConfig(targetIp).getPort();
				
				StringBuilder sb = new StringBuilder();
				sb.append(targetIp).append(":").append(port).append("/").append(servletContextName).append("/").append(serviceName).append(TerminatorConstant.CORENAME_SEPERATOR).append(groupName);
				shardurlSb.append(sb.toString()).append(",");
			}
			
			String _shardurl = shardurlSb.toString();
			String shardurl = _shardurl.substring(0,_shardurl.length()-1);
			
			if(logger.isDebugEnabled()){
				logger.debug("此此搜索是多shard搜索，其中shard-string 为 ==> " + shardurl);
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
			logger.warn("(重新)订阅HSF服务 ===> " + version);
			try {
                //TODO
				//TerminatorHSFContainer.subscribeService(TerminatorService.class.getName(), version);
			} catch (Exception e) {
				logger.error("订阅HSF服务异常 ==> " + version);
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
