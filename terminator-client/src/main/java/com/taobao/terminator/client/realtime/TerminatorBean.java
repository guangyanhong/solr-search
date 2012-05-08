package com.taobao.terminator.client.realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;

import com.taobao.terminator.client.TerminatorInitException;
import com.taobao.terminator.client.router.GroupRouter;
import com.taobao.terminator.client.router.ServiceConfigAware;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.config.GroupConfig;
import com.taobao.terminator.common.config.HostConfig;
import com.taobao.terminator.common.config.HostStatusHolder;
import com.taobao.terminator.common.config.ServiceConfig;
import com.taobao.terminator.common.config.ServiceConfigSupport;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.DeleteByQueryRequest;
import com.taobao.terminator.common.protocol.RealTimeService;
import com.taobao.terminator.common.protocol.RouteValueSupport;
import com.taobao.terminator.common.protocol.SearchService;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class TerminatorBean implements ServiceConfigSupport{
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
		this.initHostStatusHolder();
		this.subscribeHSFService();
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
		try {
			this.subscribeSearchService(serviceConfig);
		//	this.subscribeMergerService();
			this.subscribeRealTimeServices(serviceConfig);
		} catch (Exception e) {
			throw new TerminatorInitException("����HSF����ʱ����",e);
		}
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
				logger.debug("Search-Content ==> " + query.getQuery());
			}
			return this.searchServices.get("0").query(query);
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
						throw new TerminatorServiceException("����GroupName����.",e);
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
				return this.searchServices.get(groupName).query(query);
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
			
			return this.mergerService.query(query);
		}
	}
	
	private Map<String,SearchService> searchServices; //key:groupNumber
	private SearchService mergerService;
	private Map<String,RealTimeService> realTimeServices;
	private boolean serviceConfigChangeAware = false;
	
	/**
	 * ��Ⱥ�ṹ��������󣬻ص��������
	 */
	public void onServiceConfigChange(ServiceConfig serviceConfig) {
		if(serviceConfigChangeAware) {
			this.serviceConfig = serviceConfig;
			this.subscribeHSFService();
		} else {
			logger.warn("ServiceConfig �����˱仯�����Ǳ�ϵͳ����Ϊ--[����֪ServiceConfig�䶯],�ʺ��Ե�����������ϵĽṹ���ϵĽṹ����:" + this.serviceConfig +" �µĽṹ��: " + serviceConfig);
		}
	}
	
	private void subscribeSearchService(ServiceConfig serviceConfig) {
		Set<String> groupNumberSet = serviceConfig.keySet();
		if (searchServices == null) {
			searchServices = new ConcurrentHashMap<String, SearchService>();
		}

		Map<String, SearchService> tmpSearchServices = new ConcurrentHashMap<String, SearchService>();
		for (String number : groupNumberSet) {
			String version = serviceName + "-" + number + "-reader";
            //TODO
		//	Object obj = this.subscribeHSF(SearchService.class.getName(), version);
		//	tmpSearchServices.put(number, (SearchService) obj);
		}
		
		//�����������̫���ˣ���ͬ����
		this.searchServices = tmpSearchServices;
	}
	
//	private void subscribeMergerService() {
//		String version = serviceName + "-merger";
//		mergerService = (SearchService)this.subscribeHSF(SearchService.class.getName(), version);
//	}
	
	//version = coreName + "-" + TerminatorCommonUtils.getLocalHostIP()
	private void subscribeRealTimeServices(ServiceConfig serviceConfig) {
		final Set<String> numberSet = serviceConfig.keySet();
		Map<String, RealTimeService> tmpRealTimeServices = new ConcurrentHashMap<String, RealTimeService>();
		for (String number : numberSet) {
			String version = RealTimeService.Utils.genHsfVersion(this.serviceName + "-" + number);
			//Object obj = this.subscribeHSF(RealTimeService.class.getName(), version);
			//tmpRealTimeServices.put(number, (RealTimeService) obj);
		}
		this.realTimeServices = tmpRealTimeServices;
	}
	 //TODO
//	protected Object subscribeHSF(String ifname,String version) {
//		logger.warn("Subscribe hsf service {" + ifname + ":" + version +"}");
//		HSFSpringConsumerBean hsfConsumerBean = new HSFSpringConsumerBean();
//		hsfConsumerBean.setInterfaceName(ifname);
//		hsfConsumerBean.setVersion(version);
//		try {
//			hsfConsumerBean.init();
//			return hsfConsumerBean.getObject();
//		} catch (Exception e) {
//			logger.error("����HSF�쳣",e);
//			return null;
//		}
//	}
	
	public int add(final AddDocumentRequest addReq) throws TerminatorServiceException {
		return this.doRequst(new Command() {
			@Override
			public int doWork(RealTimeService rtservice) throws Exception {
				return rtservice.add(addReq);
			}

			@Override
			public String type() {
				return "add";
			}
		}, addReq.routeValue);
	}
	
	public int madd(List<AddDocumentRequest> addReqs) throws TerminatorServiceException {
		int sucCount = addReqs.size();
		Map<String,List<AddDocumentRequest>> maps = addReqSpliter.group(addReqs);
		Set<String> groupNames = maps.keySet();
		for (String groupName : groupNames) {
			RealTimeService service = this.realTimeServices.get(groupName);
			List<AddDocumentRequest> list = maps.get(groupName);
			try {
				service.madd(list);
			} catch (Throwable e) {
				sucCount = sucCount - list.size();
				logger.error("madd - ERROR",e);
			}
		}
		return sucCount;
	}

	public int mupdate(List<UpdateDocumentRequest> updateReqs) throws TerminatorServiceException {
		int sucCount = updateReqs.size();
		Map<String, List<UpdateDocumentRequest>> maps = updateReqSpliter.group(updateReqs);
		Set<String> groupNames = maps.keySet();
		for (String groupName : groupNames) {
			RealTimeService service = this.realTimeServices.get(groupName);
			List<UpdateDocumentRequest> list = maps.get(groupName);
			try {
				service.mupdate(list);
			} catch (Throwable e) {
				sucCount = sucCount - list.size();
				logger.error("mupdate - ERROR",e);
			}
		}
		return sucCount;
	}
	
	public int mdelete(List<DeleteByIdRequest> delReqs) throws TerminatorServiceException {
		int sucCount = delReqs.size();
		Map<String,List<DeleteByIdRequest>> maps = delByIdReqSpliter.group(delReqs);
		Set<String> groupNames = maps.keySet();
		for (String groupName : groupNames) {
			RealTimeService service = this.realTimeServices.get(groupName);
			List<DeleteByIdRequest> list = maps.get(groupName);
			try {
				service.mdelete(list);
			} catch (Throwable e) {
				sucCount = sucCount - list.size();
				logger.error("mdelete - ERROR",e);
			}
		}
 		return sucCount;
	}
	
	public int mdeleteByQuery(List<DeleteByQueryRequest> delReqs) throws TerminatorServiceException {
		int sucCount = delReqs.size();
		Map<String,List<DeleteByQueryRequest>> maps = delByQueryReqSpliter.group(delReqs);
		
		Set<String> groupNames = maps.keySet();
		for (String groupName : groupNames) {
			RealTimeService service = this.realTimeServices.get(groupName);
			List<DeleteByQueryRequest> list = maps.get(groupName);
			try {
				service.mdeleteByQuery(list);
			} catch (Throwable e) {
				sucCount = sucCount - list.size();
				logger.error("mdeleteByQuery - ERROR",e);
			}
		}
 		return sucCount;
	}
	
	private Spliter<AddDocumentRequest>    addReqSpliter        = new Spliter<AddDocumentRequest>();
	private Spliter<UpdateDocumentRequest> updateReqSpliter     = new Spliter<UpdateDocumentRequest>();
	private Spliter<DeleteByIdRequest>     delByIdReqSpliter    = new Spliter<DeleteByIdRequest>();
	private Spliter<DeleteByQueryRequest>  delByQueryReqSpliter = new Spliter<DeleteByQueryRequest>();
	
	@SuppressWarnings("unchecked")
	private Map<String,Spliter> spliters = new HashMap<String,Spliter>();
	{
		spliters.put("add", addReqSpliter);
		spliters.put("update", updateReqSpliter);
		spliters.put("delete", delByIdReqSpliter);
		spliters.put("deleteByQuery", delByQueryReqSpliter);
	}
	
	public class Spliter<T extends RouteValueSupport> {
		public Map<String,List<T>> group(List<T> list) throws TerminatorServiceException{
			Map<String,List<T>> maps = new HashMap<String,List<T>>();
			for(T item : list) {
				String groupNumber = getGroupNumber(item.getRouteValue());
				List<T> reqs = null;
				if((reqs = maps.get(groupNumber)) == null) {
					reqs = new ArrayList<T>();
					maps.put(groupNumber, reqs);
				}
				reqs.add(item);
			}
			return maps;
		}
	}
	
	public int update(final UpdateDocumentRequest updateReq) throws TerminatorServiceException {
		return this.doRequst(new Command() {
			@Override
			public int doWork(RealTimeService rtservice) throws Exception {
				return rtservice.update(updateReq);
			}

			@Override
			public String type() {
				return "update";
			}
		}, updateReq.routeValue);
	}

	public int delete(final DeleteByIdRequest delReq) throws TerminatorServiceException {
		return this.doRequst(new Command() {
			@Override
			public int doWork(RealTimeService rtservice) throws Exception {
				return rtservice.delete(delReq);
			}

			@Override
			public String type() {
				return "delete";
			}
		}, delReq.routeValue);
	}
	
	public int delete(final DeleteByQueryRequest delReq) throws TerminatorServiceException {
		return this.doRequst(new Command() {
			@Override
			public int doWork(RealTimeService rtservice) throws Exception {
				return rtservice.delete(delReq);
			}

			@Override
			public String type() {
				return "deleteByQuery";
			}
		}, delReq.routeValue);
	}
	
	private interface Command {
		public String type();
		public int doWork(RealTimeService rtservice) throws Exception;
	}
	
	//ͳ�ƴ���� key��ʵʱ���������   value:����Ĵ���
	private ConcurrentHashMap<String,AtomicInteger> errorMaps = new ConcurrentHashMap<String,AtomicInteger>();

	private int doRequst(Command command, Map<String, String> routeValues) throws TerminatorServiceException {
		String groupNumber = getGroupNumber(routeValues);
		int effectNum = 0;
		RealTimeService service = this.realTimeServices.get(groupNumber);
		try {
			command.doWork(service);
			effectNum = 1;
		} catch (Exception e) {
			logger.error("[RealTime-Request-Error] type :{" + command.type() + "}");
			AtomicInteger number = errorMaps.get(command.type());
			if (number == null) {
				number = new AtomicInteger();
				errorMaps.put(command.type(), number);
			}
			number.incrementAndGet();
		}
		return effectNum;
	}
	
	private String getGroupNumber(Map<String,String> routeValue) throws TerminatorServiceException {
		String groupNumber =  null;
		if(groupRouter != null && routeValue != null) {
			groupNumber = groupRouter.getGroupName(routeValue);
		} else if(this.isSingleGroup())  {
			groupNumber = "0";
		} else {
			throw new TerminatorServiceException("RouteValue is null,can not route the request!");
		}
		return groupNumber;
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

	public boolean isServiceConfigChangeAware() {
		return serviceConfigChangeAware;
	}

	public void setServiceConfigChangeAware(boolean serviceConfigChangeAware) {
		this.serviceConfigChangeAware = serviceConfigChangeAware;
	}
}
