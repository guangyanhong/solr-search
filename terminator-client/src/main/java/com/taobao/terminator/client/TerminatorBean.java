package com.taobao.terminator.client;

import java.io.IOException;
import java.text.ParseException;
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
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import com.taobao.terminator.client.index.DumperController;
import com.taobao.terminator.client.index.FetchDataExceptionHandler;
import com.taobao.terminator.client.index.FullIndexProvideJob;
import com.taobao.terminator.client.index.IncrIndexProvideJob;
import com.taobao.terminator.client.index.TerminatorIndexProvider;
import com.taobao.terminator.client.index.buffer.DataBuffer.CapacityInfo;
import com.taobao.terminator.client.index.data.DataProvider;
import com.taobao.terminator.client.index.data.procesor.DataProcessor;
import com.taobao.terminator.client.index.timer.TimeManageException;
import com.taobao.terminator.client.index.timer.ZKTimeManager;
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
import com.taobao.terminator.common.constant.IndexType;
import com.taobao.terminator.common.protocol.MasterService;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.TerminatorService;
import com.taobao.terminator.common.zk.TerminatorZkClient;
/**
 * 客户端Bean
 * 
 * @author yusen
 */
public class TerminatorBean implements ServiceConfigSupport{
	protected static Log logger = LogFactory.getLog(TerminatorBean.class);
	
	public static final int DEFAULT_ZK_TIMEOUT         = 300000;
	public static final String DEFAULT_SERVLET_CONTEXT = "terminator-search";
	
	protected int       zkTimeout          = DEFAULT_ZK_TIMEOUT;
	protected String    servletContextName = DEFAULT_SERVLET_CONTEXT;
	protected String    zkAddress;
	protected Scheduler fullscheduler          = null;
	protected Scheduler incrscheduler          = null;
	protected boolean   startupTimerTask       = true;
	protected boolean   allowMultiFullIndexProvider = false; //标记是否允许多机器同时进行全量dump
	protected FetchDataExceptionHandler fetchDataExceptionHandler = null;
	
	protected String                   serviceName       = null;
	protected ServiceConfig            serviceConfig  	 = null;
	protected GroupRouter              groupRouter       = null;
	protected HostStatusHolder         hostStatusHolder  = null;
	protected DataProcessor            dataProcessor     = null;  
	protected DataProvider             fullDataProvider  = null;
	protected DataProvider             incrDataProvider  = null;
	protected TerminatorIndexProvider  fullIndexProvider = null;
	protected TerminatorIndexProvider  incrIndexProvider = null;
	protected CapacityInfo             fullCapacityInfo  = new CapacityInfo();
	protected CapacityInfo             incrCapacityInfo  = new CapacityInfo();
	protected TerminatorZkClient       zkClient          = null;  
	protected boolean                  canConnectToZK    = false;
	protected String                   fullCronExpression= null;
	protected String                   incrCronExpression= null;

	public void init() throws TerminatorInitException{
		//0.成员变量校验
		this.checkFields();
		
		//1.初始化ServiceConfig
		this.initServiceConfig();
		
		//2.初始化GroupRoute对象
		this.initGroupRouter();
		
		//3.订阅HSF服务
		this.subscribeHSFService();
		
		//4.机器状态的保存和监听对象
		this.initHostStatusHolder();
		
		//5.创建用于时间管理的ZKTimeManager
		this.createTimeManager();
		
		//6.初始化分布式任务管理器
		this.initDumperController();
		
		//7.初始化IndexProvider
		this.initIndexProviderAndStartTask();

	}
	
	private void initDumperController(){
		logger.warn("初始化分布式任务管理 器(ZooKeeper方式实现)");
		DumperController.createInstance(allowMultiFullIndexProvider, zkClient, serviceName);
	}
	
	private void checkFields() throws TerminatorInitException{
		if(StringUtils.isBlank(serviceName)){
			throw new TerminatorInitException("[成员属性验证 ] serviceName不能为空.");
		}
		if(StringUtils.isBlank(zkAddress)){
			throw new TerminatorInitException("[成员属性验证 ] zkAddress不能为空.");
		}
		if(StringUtils.isBlank(fullCronExpression) && fullDataProvider  != null){
			throw new TerminatorInitException("[成员属性验证 ] 配置了全量的DataProvider 故fullCronExpression不能为空.");
		}
		if(StringUtils.isBlank(incrCronExpression) && incrDataProvider != null){
			throw new TerminatorInitException("[成员属性验证 ] 配置了增量的DataProvider 故incrCronExpression不能为空.");
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
	
	private void createTimeManager()  throws TerminatorInitException{
		try {
			ZKTimeManager.createInstance(zkClient, serviceName);
		} catch (TimeManageException e) {
			throw new TerminatorInitException("创建用于时间管理的对象ZKTimeManager失败",e);
		}
	}
	
	public void triggerFullDumpJob(){
		logger.warn("手动、主动触发全量dump任务.");
		if(fullIndexProvider == null){
			throw new UnsupportedOperationException("FullIndexProvider为null,故不支持此方法.");
		}
		fullIndexProvider.dump();
	}
	
	public void triggerIncrDumpJob(){
		logger.warn("手动、主动触发增量dump任务.");
		if(fullIndexProvider == null){
			throw new UnsupportedOperationException("IncrIndexProvider为null,故不支持此方法.");
		}
		incrIndexProvider.dump();
	}
	
	protected void initIndexProviderAndStartTask()throws TerminatorInitException{
		boolean hasFull = false;
		boolean hasIncr = false;
		
		JobDetail   fullJobDetail = null;
		JobDetail   incrJobDetail = null;
		CronTrigger fullTrigger   = null;
		CronTrigger incrTrigger   = null;
		
		if(fullDataProvider != null){
			hasFull = true;
			fullIndexProvider = new TerminatorIndexProvider();
			fullIndexProvider.setCapacityInfo(fullCapacityInfo);
			fullIndexProvider.setDataProcessor(dataProcessor);
			fullIndexProvider.setDataProvider(fullDataProvider);
			fullIndexProvider.setRouter(groupRouter);
			fullIndexProvider.setServiceConfig(serviceConfig);
			fullIndexProvider.setServiceName(serviceName);
			fullIndexProvider.setIndexType(IndexType.FULL);
			fullIndexProvider.setFetchDataExceptionHandler(fetchDataExceptionHandler);
			fullIndexProvider.afterPropertiesSet();
			
			fullJobDetail = new JobDetail(this.getServiceName() + "-fullJobDetail", Scheduler.DEFAULT_GROUP, FullIndexProvideJob.class);
			fullJobDetail.getJobDataMap().put(FullIndexProvideJob.INDEX_PROVIDER_NAME, fullIndexProvider);
			fullJobDetail.getJobDataMap().put("fullServiceName", this.serviceName);
			fullTrigger = new CronTrigger();
			fullTrigger.setName(this.getServiceName() + "-fullTrigger");
			try {
				fullTrigger.setCronExpression(fullCronExpression);
			} catch (ParseException e) {
				throw new TerminatorInitException("全量的Cron时间表达式可c能有问题 ==> " + fullCronExpression,e);
			}
		}else{
			logger.warn(">>>>>>>>> 客户端没有配置全量的DataProvider,如有需要请配置该DataProvider.");
		}
		
		if(incrDataProvider != null){
			hasIncr = true;
			incrIndexProvider = new TerminatorIndexProvider();
			incrIndexProvider.setCapacityInfo(incrCapacityInfo);
			incrIndexProvider.setDataProcessor(dataProcessor);
			incrIndexProvider.setDataProvider(incrDataProvider);
			incrIndexProvider.setRouter(groupRouter);
			incrIndexProvider.setServiceConfig(serviceConfig);
			incrIndexProvider.setServiceName(serviceName);
			incrIndexProvider.setIndexType(IndexType.INCREMENT);
			incrIndexProvider.setFetchDataExceptionHandler(fetchDataExceptionHandler);
			incrIndexProvider.afterPropertiesSet();
			
			incrJobDetail = new JobDetail(this.getServiceName() + "-incrJobDetail", Scheduler.DEFAULT_GROUP, IncrIndexProvideJob.class);
			incrJobDetail.getJobDataMap().put(IncrIndexProvideJob.INDEX_PROVIDER_NAME, incrIndexProvider);
			incrJobDetail.getJobDataMap().put("incrServiceName", this.serviceName);
			incrTrigger = new CronTrigger();
			incrTrigger.setName(this.getServiceName() + "-incrTrigger");
			try {
				incrTrigger.setCronExpression(incrCronExpression);
			} catch (ParseException e) {
				throw new TerminatorInitException("增量的Cron时间表达式可能有问题 ==>" + incrCronExpression,e);
			}
		}else{
			logger.warn(">>>>>>>>> 客户端没有配置增量的DataProvider,如有需要请配置该DataProvider.");
		}
		
		if(!startupTimerTask){
			logger.warn(" >>>>>>>>>>>>>>>>>  不启动增量 、全量时间任务   <<<<<<<<<<<<<<<<<<<<<<");
			return ;
		}
		
		if(hasIncr || hasFull){
			try {
				fullscheduler = StdSchedulerFactory.getDefaultScheduler();
				incrscheduler = StdSchedulerFactory.getDefaultScheduler();
				logger.warn("增量dump调度器 " + incrscheduler.getSchedulerName());
				logger.warn("全量dump调度器 " + fullscheduler.getSchedulerName());
				if(hasFull){
					fullscheduler.scheduleJob(fullJobDetail, fullTrigger);
				}
				if(hasIncr){
					incrscheduler.scheduleJob(incrJobDetail, incrTrigger);
				}
			} catch (SchedulerException e) {
				throw new TerminatorInitException("设置时间任务调度失败",e);
			}
			
			try {
				fullscheduler.start();
				incrscheduler.start();
			} catch (SchedulerException e) {
				throw new TerminatorInitException("启动时间任务失败",e);
			}
		}else{
			logger.warn(">>>>>>>>> 客户端及没有配置增量的DataProvider又没有配置全量的DataProvider,请配置.");
		}
	}
	
	public void shutdownFullDumpJob(boolean waitToFinish) throws SchedulerException{
		fullscheduler.shutdown(waitToFinish);
	}
	
	public void shutdownIncrDumpJob(boolean waitToFinish) throws SchedulerException{
		incrscheduler.shutdown(waitToFinish);
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
       // return TerminatorHSFContainer.getTerminatorService(serviceType, coreName);
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
		//1.订阅TerminatorService
		Set<String> versionSet = TerminatorHSFContainer.Utils.generateTerminatorVersions(this.serviceConfig.getCoreNameSet());
		for(String version : versionSet){
			logger.warn("(重新)订阅HSF服务 ===> " + version);
			try {
				//TODO:TerminatorHSFContainer.subscribeService(TerminatorService.class.getName(), version);
			} catch (Exception e) {
				logger.error("订阅HSF服务异常 ==> " + version);
				throw new TerminatorInitException(e);
			}
		}
		
		//2订阅MasterService
		Set<String> versionSet2 = TerminatorHSFContainer.Utils.generateMasterVersions(this.serviceConfig.getCoreNameSet());
		for(String version : versionSet2){
			logger.warn("(重新)订阅HSF服务 ===> " + version);
			try {
                //TODO:TerminatorHSFContainer.subscribeService(MasterService.class.getName(), version);
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

	public DataProcessor getDataProcessor() {
		return dataProcessor;
	}

	public void setDataProcessor(DataProcessor dataProcessor) {
		this.dataProcessor = dataProcessor;
	}

	public DataProvider getFullDataProvider() {
		return fullDataProvider;
	}

	public void setFullDataProvider(DataProvider fullDataProvider) {
		this.fullDataProvider = fullDataProvider;
	}

	public DataProvider getIncrDataProvider() {
		return incrDataProvider;
	}

	public void setIncrDataProvider(DataProvider incrDataProvider) {
		this.incrDataProvider = incrDataProvider;
	}

	public TerminatorIndexProvider getFullIndexProvider() {
		return fullIndexProvider;
	}

	public void setFullIndexProvider(TerminatorIndexProvider fullIndexProvider) {
		this.fullIndexProvider = fullIndexProvider;
	}

	public TerminatorIndexProvider getIncrIndexProvider() {
		return incrIndexProvider;
	}

	public void setIncrIndexProvider(TerminatorIndexProvider incrIndexProvider) {
		this.incrIndexProvider = incrIndexProvider;
	}

	public CapacityInfo getFullCapacityInfo() {
		return fullCapacityInfo;
	}

	public void setFullCapacityInfo(CapacityInfo fullCapacityInfo) {
		this.fullCapacityInfo = fullCapacityInfo;
	}

	public CapacityInfo getIncrCapacityInfo() {
		return incrCapacityInfo;
	}

	public void setIncrCapacityInfo(CapacityInfo incrCapacityInfo) {
		this.incrCapacityInfo = incrCapacityInfo;
	}

	public String getFullCronExpression() {
		return fullCronExpression;
	}

	public void setFullCronExpression(String fullCronExpression) {
		this.fullCronExpression = fullCronExpression;
	}

	public String getIncrCronExpression() {
		return incrCronExpression;
	}

	public void setIncrCronExpression(String incrCronExpression) {
		this.incrCronExpression = incrCronExpression;
	}

	public boolean isCanConnectToZK() {
		return canConnectToZK;
	}

	public boolean isAllowMultiFullIndexProvider() {
		return allowMultiFullIndexProvider;
	}

	public void setAllowMultiFullIndexProvider(boolean allowMultiFullIndexProvider) {
		this.allowMultiFullIndexProvider = allowMultiFullIndexProvider;
	}

	public boolean isStartupTimerTask() {
		return startupTimerTask;
	}

	public void setStartupTimerTask(boolean startupTimerTask) {
		this.startupTimerTask = startupTimerTask;
	}

	public FetchDataExceptionHandler getFetchDataExceptionHandler() {
		return fetchDataExceptionHandler;
	}

	public void setFetchDataExceptionHandler(
			FetchDataExceptionHandler fetchDataExceptionHandler) {
		this.fetchDataExceptionHandler = fetchDataExceptionHandler;
	}
}
