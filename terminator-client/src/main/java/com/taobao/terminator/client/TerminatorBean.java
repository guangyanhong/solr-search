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
 * �ͻ���Bean
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
	protected boolean   allowMultiFullIndexProvider = false; //����Ƿ���������ͬʱ����ȫ��dump
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
		//0.��Ա����У��
		this.checkFields();
		
		//1.��ʼ��ServiceConfig
		this.initServiceConfig();
		
		//2.��ʼ��GroupRoute����
		this.initGroupRouter();
		
		//3.����HSF����
		this.subscribeHSFService();
		
		//4.����״̬�ı���ͼ�������
		this.initHostStatusHolder();
		
		//5.��������ʱ������ZKTimeManager
		this.createTimeManager();
		
		//6.��ʼ���ֲ�ʽ���������
		this.initDumperController();
		
		//7.��ʼ��IndexProvider
		this.initIndexProviderAndStartTask();

	}
	
	private void initDumperController(){
		logger.warn("��ʼ���ֲ�ʽ������� ��(ZooKeeper��ʽʵ��)");
		DumperController.createInstance(allowMultiFullIndexProvider, zkClient, serviceName);
	}
	
	private void checkFields() throws TerminatorInitException{
		if(StringUtils.isBlank(serviceName)){
			throw new TerminatorInitException("[��Ա������֤ ] serviceName����Ϊ��.");
		}
		if(StringUtils.isBlank(zkAddress)){
			throw new TerminatorInitException("[��Ա������֤ ] zkAddress����Ϊ��.");
		}
		if(StringUtils.isBlank(fullCronExpression) && fullDataProvider  != null){
			throw new TerminatorInitException("[��Ա������֤ ] ������ȫ����DataProvider ��fullCronExpression����Ϊ��.");
		}
		if(StringUtils.isBlank(incrCronExpression) && incrDataProvider != null){
			throw new TerminatorInitException("[��Ա������֤ ] ������������DataProvider ��incrCronExpression����Ϊ��.");
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
	
	private void createTimeManager()  throws TerminatorInitException{
		try {
			ZKTimeManager.createInstance(zkClient, serviceName);
		} catch (TimeManageException e) {
			throw new TerminatorInitException("��������ʱ�����Ķ���ZKTimeManagerʧ��",e);
		}
	}
	
	public void triggerFullDumpJob(){
		logger.warn("�ֶ�����������ȫ��dump����.");
		if(fullIndexProvider == null){
			throw new UnsupportedOperationException("FullIndexProviderΪnull,�ʲ�֧�ִ˷���.");
		}
		fullIndexProvider.dump();
	}
	
	public void triggerIncrDumpJob(){
		logger.warn("�ֶ���������������dump����.");
		if(fullIndexProvider == null){
			throw new UnsupportedOperationException("IncrIndexProviderΪnull,�ʲ�֧�ִ˷���.");
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
				throw new TerminatorInitException("ȫ����Cronʱ����ʽ��c�������� ==> " + fullCronExpression,e);
			}
		}else{
			logger.warn(">>>>>>>>> �ͻ���û������ȫ����DataProvider,������Ҫ�����ø�DataProvider.");
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
				throw new TerminatorInitException("������Cronʱ����ʽ���������� ==>" + incrCronExpression,e);
			}
		}else{
			logger.warn(">>>>>>>>> �ͻ���û������������DataProvider,������Ҫ�����ø�DataProvider.");
		}
		
		if(!startupTimerTask){
			logger.warn(" >>>>>>>>>>>>>>>>>  ���������� ��ȫ��ʱ������   <<<<<<<<<<<<<<<<<<<<<<");
			return ;
		}
		
		if(hasIncr || hasFull){
			try {
				fullscheduler = StdSchedulerFactory.getDefaultScheduler();
				incrscheduler = StdSchedulerFactory.getDefaultScheduler();
				logger.warn("����dump������ " + incrscheduler.getSchedulerName());
				logger.warn("ȫ��dump������ " + fullscheduler.getSchedulerName());
				if(hasFull){
					fullscheduler.scheduleJob(fullJobDetail, fullTrigger);
				}
				if(hasIncr){
					incrscheduler.scheduleJob(incrJobDetail, incrTrigger);
				}
			} catch (SchedulerException e) {
				throw new TerminatorInitException("����ʱ���������ʧ��",e);
			}
			
			try {
				fullscheduler.start();
				incrscheduler.start();
			} catch (SchedulerException e) {
				throw new TerminatorInitException("����ʱ������ʧ��",e);
			}
		}else{
			logger.warn(">>>>>>>>> �ͻ��˼�û������������DataProvider��û������ȫ����DataProvider,������.");
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
		//1.����TerminatorService
		Set<String> versionSet = TerminatorHSFContainer.Utils.generateTerminatorVersions(this.serviceConfig.getCoreNameSet());
		for(String version : versionSet){
			logger.warn("(����)����HSF���� ===> " + version);
			try {
				//TODO:TerminatorHSFContainer.subscribeService(TerminatorService.class.getName(), version);
			} catch (Exception e) {
				logger.error("����HSF�����쳣 ==> " + version);
				throw new TerminatorInitException(e);
			}
		}
		
		//2����MasterService
		Set<String> versionSet2 = TerminatorHSFContainer.Utils.generateMasterVersions(this.serviceConfig.getCoreNameSet());
		for(String version : versionSet2){
			logger.warn("(����)����HSF���� ===> " + version);
			try {
                //TODO:TerminatorHSFContainer.subscribeService(MasterService.class.getName(), version);
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
