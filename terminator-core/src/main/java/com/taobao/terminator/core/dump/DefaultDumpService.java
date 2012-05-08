package com.taobao.terminator.core.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.SchedulingException;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.common.data.DataProvider;
import com.taobao.terminator.common.data.filter.GroupFilter;
import com.taobao.terminator.common.data.processor.DataProcessor;
import com.taobao.terminator.common.data.timer.FileTimerManager;
import com.taobao.terminator.common.data.timer.TimerManager;
import com.taobao.terminator.common.protocol.FetchFileListResponse;
import com.taobao.terminator.common.stream.FileGetClient;
import com.taobao.terminator.common.stream.FileGetResponse;
import com.taobao.terminator.common.stream.FileGetServer;
import com.taobao.terminator.common.zk.TerminatorZkClient;
import com.taobao.terminator.common.zk.lock.ZKLock;
import com.taobao.terminator.core.ContextInfo;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.realtime.TerminatorUpdateHandler;
import com.taobao.terminator.core.service.ZkClientHolder;
import com.taobao.terminator.core.util.IndexFileUtils;
import com.taobao.terminator.core.util.IndexUtils;

public class DefaultDumpService implements NamedListInitializedPlugin,DumpService{
	
	public final static String MASTER_WAIT_TIME   = "masterWaitTime";
	public final static String FILE_GET_PORT  	  = "fileGetPort";
	public final static String FULL_DATA_PROVIDER = "fullDataProvider";
	public final static String INCR_DATA_PROVIDER = "incrDataProvider";
	public final static String DATA_PROCESSOR 	  = "dataProcessor";
	public final static String GROUP_FILTER 	  = "gruopFilter";
	public final static String TIMER_MANAGER 	  = "timerManager";
	public final static String INCR_TIME_FILE 	  = "incr_time_file.txt";
	
	private static final String FULL_INDEX_JOB_NAME              = "-FullIndexJob";
	private static final String INCR_INDEX_JOB_NAME              = "-IncrIndexJob";
	private static final String INDEX_JOB_GROUP                  = "-index";
	private static final String INCR_INDEX_JOB_TRIGGER_NAME      = "-IncrIndexTrigger";
	private static final String JOB_TRIGGER_GROUP                = "-trigger";
	private static final String FULL_INDEX_JOB_TRIGGER_NAME 	 = "-FullIndexTrigger";
	
	private String incrCronExpression          = "0 0/2 * * * ?";
	private String fullCronExpression          = "0 0 2 * * ?";
	
	private final Log log = LogFactory.getLog(DefaultDumpService.class);
	
	@SuppressWarnings("unchecked")
	private NamedList 				args = null; 
	private SolrCore 				solrCore = null;
	private Map<String,DumpService> dumpServices;      //���߳̾����Ļ���̫���ˣ��Ͳ�ͬ��������
	private ApplicationContext 		applicationContext;
	private ZKLock 					lock;
	private String 					coreName;
	private FullIndexProvider 		fullIndexProvider;
	private IncrIndexProvider 		incrIndexProvider;
	private String 					incrTimeFilePath;
	private Scheduler 				indexScheduler;
	private FileGetServer 			fileServer;
	private FileGetClient 			fileClient;
	private int 					fileGetServerPort = 12508;
	private CountDownLatch          countDownLatch ;
	private int                     masterWaitTime = 60 * 60 * 1000;
	private TerminatorZkClient      zkClient;
	private boolean  				containsIncrDP;
	
	
	public DefaultDumpService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.coreName = solrCore.getName();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void init(NamedList args) {
		this.args = args;
		
		this.initLock();
		
		this.loadSpringContext();
		
		this.initIndexProviders();
		
		try {
			this.initScheduler();
		} catch (Exception e) {
			throw new InitException("��ʼ����ʱ����ʧ��", e);
		}
		
		//this.publishService();
		
		//this.subscribeService();
		
		this.startFileGetServer();
	}
	
	private void initLock(){
		this.zkClient = ZkClientHolder.zkClient;
		this.lock = new ZKLock("Terminator" + solrCore.getName(),"Dumper",this.zkClient);
	}
	
	/**
	 * ����ָ����spring�����ļ�
	 */
	private void loadSpringContext() {
		SolrResourceLoader resourceLoader = solrCore.getResourceLoader();
		
		String springConfig = (String)(this.args.get(TerminatorContextLoader.SPRING_CONFIGS));
		if(StringUtils.isBlank(springConfig)){
			throw new InitException("[" + coreName + "]" + "û������Service���� ==> " + TerminatorContextLoader.SPRING_CONFIGS + ",�ʲ��ܼ���Spring����.");
		}
		
		log.warn("[" + coreName + "] Spring�����ļ�Ϊ ==> " + springConfig);
		String[] springConfigArray = springConfig.split(",");
		List<String> springConfigs = new ArrayList<String>(springConfigArray.length);
		for(String config : springConfigArray){
			springConfigs.add(config);
		}
		
		String confDir = resourceLoader.getConfigDir();
		
		URLClassLoader urlClassLoader = null;
		try {
			Field field = resourceLoader.getClass().getDeclaredField("classLoader"); //���÷������SolrResourceLoader��private classLoader
			field.setAccessible(true);
			urlClassLoader = (URLClassLoader)field.get(resourceLoader);
		} catch (Exception e) {
			throw new InitException("[" + coreName + "]" + "[loadSpringContext] --> ����Spring�����Ƿ����ȡSolrResourceLoader�����private����classLoaderʧ��",e);
		}
		
		Map<String, Object> properties = new ContextInfo(solrCore);
		TerminatorContextLoader terminatorContextLoader = new TerminatorContextLoader(confDir, springConfigs, properties, urlClassLoader);
		
		try {
			terminatorContextLoader.init();
		} catch(Exception e) {
			throw new InitException("[" + coreName + "]" + "[loadSpringContext] --> TerminatorContextLoader����Spring����ʧ��",e);
		}
		this.applicationContext = terminatorContextLoader.getApplicationContext();
	}
	
	
	private void initIndexProviders() {
		
		this.containsIncrDP = this.applicationContext.containsBean(INCR_DATA_PROVIDER);
		
		this.fullIndexProvider = new FullIndexProvider();
		if(containsIncrDP){
			this.incrIndexProvider = new IncrIndexProvider();
		}
		
		this.fullIndexProvider.setDataProvider((DataProvider)this.applicationContext.getBean(FULL_DATA_PROVIDER));
		this.fullIndexProvider.setSolrCore(solrCore);

		if(containsIncrDP){
			this.incrIndexProvider.setDataProvider((DataProvider)this.applicationContext.getBean(INCR_DATA_PROVIDER));
			this.incrIndexProvider.setSolrCore(solrCore);
		}
		
		DataProcessor processors = null;
		try {
			processors = (DataProcessor)this.applicationContext.getBean(DATA_PROCESSOR);
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "�û�û�ж���DataProcessor");
		}
		if(processors != null) {
			this.fullIndexProvider.setDataProcessor(processors);
			if(containsIncrDP){
				this.incrIndexProvider.setDataProcessor(processors);
			}
		}
		
		GroupFilter groupFilter = null;
		try {
			groupFilter = (GroupFilter)this.applicationContext.getBean(GROUP_FILTER);
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "�û�û�ж���GroupFilter");
		}
		
		
		if(groupFilter != null) {
			this.fullIndexProvider.setGroupFilter(groupFilter);
			if(containsIncrDP){
				this.incrIndexProvider.setGroupFilter(groupFilter);
			}
		}
		
		if(containsIncrDP){
			this.incrTimeFilePath = solrCore.getCoreDescriptor().getCoreContainer().getSolrHome() + File.separatorChar + solrCore.getName() + File.separatorChar + IndexEnum.INCR_START_TIME_FILE.getValue();
			TimerManager timerManager = null;
			try {
				timerManager = (TimerManager)this.applicationContext.getBean(TIMER_MANAGER);
			} catch(Exception e) {
				log.warn("[" + coreName + "]" + "�û�û�����ж���TimerManager,��ʹ��Ĭ�ϵ�FileTimerManagerʵ��.");
			}
			
			if(timerManager != null) {
				this.incrIndexProvider.setTimerManager(timerManager);
			} else {
				this.incrIndexProvider.setTimerManager(new FileTimerManager(this.incrTimeFilePath));
			}
		}
	}
	
	private void initScheduler() throws SchedulerException, ParseException {
		DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
		RAMJobStore jobStore = new RAMJobStore();
		SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
		jobStore.setMisfireThreshold(60000);
		schedulerFactory.createScheduler(this.solrCore.getName() + "-scheduler", this.solrCore.getName() + "-scheulerInstance", threadPool, jobStore);
		threadPool.initialize();
		this.indexScheduler = schedulerFactory.getScheduler(this.solrCore.getName() + "-scheduler");
		
		JobDetail fullIndexJobDetail = new JobDetail(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, FullDumpJob.class);
		JobDetail incrIndexJobDetail = new JobDetail(this.solrCore.getName() + INCR_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, IncrDumpJob.class);
		
		String s = (String)this.args.get("fullCronExpression");
		if(s != null && StringUtils.isNotBlank(s)){
			this.fullCronExpression = s;
		}
		
		s = (String)this.args.get("incrCronExpression");
		if(s != null && StringUtils.isNotBlank(s)){
			this.incrCronExpression = s;
		}
		
		boolean needIncr = StringUtils.isNotBlank(s) &&  containsIncrDP;
		
		log.warn("[" + coreName + "] ȫ�����������Cron����ʽ [" + this.fullCronExpression + "],�������������Cron����ʽ [" + this.incrCronExpression + "]");
		
		
		CronTrigger incrJobTrigger = null;
		if(needIncr){
			incrJobTrigger = new CronTrigger(this.solrCore.getName() + INCR_INDEX_JOB_TRIGGER_NAME, this.solrCore.getName() + JOB_TRIGGER_GROUP, this.incrCronExpression);
			this.indexScheduler.getContext().put(DumpService.JOB_INCR_INDEX_PROVIDER, this.incrIndexProvider);
			this.indexScheduler.addJobListener(new IncrIndexJobListener());
			incrIndexJobDetail.addJobListener("IncrIndexJobListener");
		}
		
		CronTrigger fullJobTrigger = new CronTrigger(this.solrCore.getName() + FULL_INDEX_JOB_TRIGGER_NAME, this.solrCore.getName() + JOB_TRIGGER_GROUP, this.fullCronExpression);
		this.indexScheduler.getContext().put(DumpService.JOB_SOLR_CORE, this.solrCore);
		this.indexScheduler.getContext().put(DumpService.JOB_FULL_INDEX_PROVIDER, this.fullIndexProvider);
		this.indexScheduler.addJobListener(new FullIndexJobListener());
		fullIndexJobDetail.addJobListener("FullIndexJobListener");
		
		this.start();
		
		if(needIncr){
			this.indexScheduler.scheduleJob(incrIndexJobDetail, incrJobTrigger);	
		}
		this.indexScheduler.scheduleJob(fullIndexJobDetail, fullJobTrigger);
	}
	
	public void start() throws SchedulingException {
		if (this.indexScheduler != null) {
			try {
				this.indexScheduler.start();
			}
			catch (SchedulerException ex) {
				throw new SchedulingException("Could not start Quartz Scheduler", ex);
			}
		}
	}
	
	private void startFileGetServer() {
		String portStr = (String)args.get(FILE_GET_PORT);
		if(portStr != null){
			fileGetServerPort = Integer.valueOf(portStr);
		}
		log.warn("����FileGetServer,host ==> 127.0.0.1  port ==> " + fileGetServerPort);
		this.fileServer = new FileGetServer(TerminatorCommonUtils.getLocalHostIP(), fileGetServerPort);
		
		try {
			if(!fileServer.isAlive()){
				fileServer.start();
				this.fileGetServerPort = fileServer.getPort();
				log.warn("FileGetServer�Ķ˿�Ϊ ==> " + this.fileGetServerPort);
			}
		} catch (IOException e) {
			log.error("����FileGetServerʧ��",e);
			throw new InitException("����FilGetServerʧ��",e);
		}
	}
	
//	/**
//	 * �������󷢲�ΪHSF��������Э�����õķ���
//	 */
//	private void publishService(){
//		String version = this.getVersion(TerminatorCommonUtils.getLocalHostIP());
//		log.warn("[" + coreName + "]" + " ����HSF����,Version ===> " + version);
//		HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//
//		providerBean.setTarget(this);
//		providerBean.setServiceInterface(DumpService.class.getName());
//		providerBean.setServiceVersion(version);
//		providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//		providerBean.setClientTimeout(3000);
//
//		try {
//			providerBean.init();
//		} catch (Exception e) {
//			throw new InitException("[" + coreName + "]" + "����HSF����ʧ��,version ==> " + version, e);
//		}
//
//		String dataId = this.getDataId();
//		String groupId = this.getGroupId();
//
//		log.warn("[" + coreName + "] �����ڲ�DataId ==> " + dataId + " GroupId ==> " + groupId +  "Data ==> " + version);
//
//		PublisherRegistration<String> registration = new PublisherRegistration<String>("Terminator-InnerService-Publisher",dataId );
//		registration.setGroup(groupId);
//		Publisher<String> publisher =  PublisherRegistrar.register(registration);
//		publisher.publish(version);
//	}
	
	private boolean isLocalService(String service){
		return service.contains(TerminatorCommonUtils.getLocalHostIP());
	}
	
//	/**
//	 * �������ڷ���
//	 */
//	private void subscribeService(){
//		SubscriberRegistration registration = new SubscriberRegistration("Terminator-InnerService-Subscriber", this.getDataId());
//		registration.setGroup(getGroupId());
//
//		Subscriber subscriber = SubscriberRegistrar.register(registration);
//		dumpServices = new HashMap<String,DumpService>();
//
//		subscriber.setDataObserver(new SubscriberDataObserver() {
//			@Override
//			public void handleData(String dataId, List<Object> datas) {
//
//				Set<String> newVersions = new HashSet<String>();
//				for (Object o : datas) {
//					newVersions.add((String) o);
//				}
//
//				log.warn("���ĵ�����,dataId ==> " + dataId  + " �б仯�������Ѿ����͹���...==>" + newVersions);
//
//				for(String newVersion : newVersions){
//					subscribeService(newVersion);
//				}
//
//				Set<String> versions = dumpServices.keySet();
//				for(String version : versions){
//					if(!newVersions.contains(version)){
//						dumpServices.remove(version);
//						log.warn("[" + coreName + "] �Ƴ�ʧЧ���ڲ�ͨѶ���� ��Version ==> " + version);
//					}
//				}
//
//				log.warn("\n\n\n[" + coreName + "] ���ڶ��ĵ��ڲ������� ==> " + dumpServices.keySet() + "\n\n\n");
//
//
//				/*Set<String> intersection = new HashSet<String>();
//
//				for (String e : newVersions) {
//					if (oldVersions.contains(e)) {
//						intersection.add(e);
//					}
//				}
//
//				if (intersection.size() > 0) {
//					newVersions.removeAll(intersection);
//					oldVersions.removeAll(intersection);
//				}
//
//				Set<String> addVersions    = newVersions;  //�����ķ����Versions
//				Set<String> removeVersions = oldVersions;  //��Ҫ�����ķ����ĵ�Versions
//				Set<String> retainVersions = intersection; //���ֲ���ķ����ĵ�Versions
//
//
//				if(addVersions != null  && !addVersions.isEmpty()){
//					for(String addVersion : addVersions){
//						subscribeService(addVersion);
//					}
//				}
//
//				if(removeVersions != null && !removeVersions.isEmpty()){
//					for(String removeVersion : removeVersions){
//						dumpServices.remove(removeVersion);
//					}
//				}
//
//				log.warn("=========> ��̨�������ڵ��ڲ�ͨѶ����Ϊ : " + dumpServices.keySet());
//
//				oldVersions = dumpServices.keySet();
//
//				log.warn(" \n �����ķ���Version ==> " + addVersions +
//						 " \n ��Ҫ�����ķ���Versions ==> " + removeVersions +
//						 " \n ���ֲ���ķ���Versions ==> " + retainVersions +
//						 " \n ���ڵ�Versions ==> " + oldVersions);*/
//			}
//		});
//	}
	
	
//	private void subscribeService(String version){
//		log.warn("[" + coreName + "]" + "����HSF����,Version ===> " + version);
//
//		if(isLocalService(version)){
//			log.warn("[" + coreName + "]" +  "Version ==> " + version + " �ķ���ò���Ǳ������񣬹ʲ���Ҫ���Ĵ˷���.");
//			return ;
//		}
//
//		if(dumpServices.containsKey(version)){
//			log.warn("[" + coreName + "] �ظ����ģ��Ѿ�����VersionΪ " +version + " �ķ���.");
//			return;
//		}
//
//		HSFSpringConsumerBean hsfConsumerBean = new HSFSpringConsumerBean();
//
//		hsfConsumerBean.setInterfaceName(DumpService.class.getName());
//		hsfConsumerBean.setVersion(version);
//		DumpService dumpService = null;
//		try {
//			hsfConsumerBean.init();
//			dumpService = (DumpService)hsfConsumerBean.getObject();
//			dumpServices.put(version, dumpService);
//		} catch (Exception e) {
//			log.error("[" + coreName + "]" + "����HSF����ʧ��,Version ===> " + version,e);
//		}
//	}
	
	private String getVersion(String ip){
		StringBuilder sb = new StringBuilder();
		sb.append("Terminator-InnserService-").append(this.solrCore.getName()).append("-").append(ip);
		return sb.toString();
	}
	
	private String getDataId(){
		return coreName + "-innerservice";
	}
	
	private String getGroupId(){
		return "Terminator-Inner";
	}
	
	/**
	 * Master��ɫȫ��Dump��Ϻ�֪ͨSlave��ɫ�Ļ���������Slave��ɫ�Ļ�����Master����ȫ����������ļ�
	 * 
	 * @param masterIp
	 * @param fileNames
	 */
	public boolean notifyToSlave(String masterIp, int port,String[] fileNames, String incrTime){
		FetchFileListResponse fileListResponse = new FetchFileListResponse(masterIp, port, Arrays.asList(fileNames));
		log.warn(" [" + solrCore.getName() + "] Master����ȫ���ɹ�������ϣ�����(Slave)��ʼ��Master������ȫ����������ļ�. ==>" + fileListResponse.toString());
		Thread thread = new Thread(new FechFullIndexFilesJob(fileListResponse, incrTime));
		thread.setName("FetchIndexFileFromMaster-Thread");
		thread.start();
		return true;
	}
	
	/**
	 * Slave��ɫ�Ļ�����Master��ɫ�Ļ����ر�����ȫ�������������
	 * 
	 * @param slaveIp
	 * @param msg
	 */
	public boolean reportToMaster(String slaveIp,String msg){
		if(countDownLatch == null){
			log.warn("[" + coreName + "] Slave��Master�㱨��Ϣ,���Ǵ�ʱCountDownLatch == null,�ʴ˴�������Ч.");
			return false;
		}
		
		countDownLatch.countDown();
		long currentCount = countDownLatch.getCount();
		log.warn("[" + coreName + "]" + "Slave��Master�㱨��Ϣ,IP===>" + slaveIp + "MSG ==> " + msg  + "Ŀǰ���� [" + currentCount + "] ��Slaveû����Master�㱨.");
		if(currentCount == 0){
			log.warn("[" + coreName + "] ����Slave�Ѿ�ȫ��Copy������ϣ�Master����������������.");
		}
		return true;
	}
	
	/**
	 * ȫ����������Listener
	 */
	private class FullIndexJobListener implements JobListener {

		@Override
		public String getName() {
			return "FullIndexJobListener";
		}

		@Override
		public void jobExecutionVetoed(JobExecutionContext context) {
			
		}

		@Override
		public void jobToBeExecuted(JobExecutionContext context) {
			log.warn("[" + coreName + "] ����ȫ����������.");
			if(lock.tryLock()) {
				context.getJobDetail().getJobDataMap().put(DumpService.JOB_CAN_EXECUTE, "yes");
				log.warn("��ȡ�ֲ�ʽLock�ɹ�.");
			} else{
				context.getJobDetail().getJobDataMap().put(DumpService.JOB_CAN_EXECUTE, "no");
				log.warn("��ȡ�ֲ�ʽLockʧ��.");
			}
			context.getJobDetail().getJobDataMap().put("coreName", solrCore.getName());
		}

		@Override
		public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
 			boolean executeSuccess = jobException == null;
			log.warn("[Full-Index] -- ["+ solrCore.getName() + "] ȫ������dump�������  ==> " +(executeSuccess ? "[����]":"[�쳣]") + "����.");
			
			if(!executeSuccess) {
				log.error("[" + coreName + "]" + "����ȫ������ִ�г��������쳣�����ܽ��к�������", jobException);
				return;
			}
			
			SolrCore oldCore = null;
			SolrCore newCore = null;
			String beginTime = TerminatorCommonUtils.formatDate(new Date());
			try {
				oldCore = (SolrCore)context.getScheduler().getContext().get(DumpService.JOB_SOLR_CORE);
				newCore = (SolrCore)context.getScheduler().getContext().get(DumpService.JOB_NEW_SOLR_CORE);
				beginTime = context.getScheduler().getContext().getString(DumpService.FULL_INDEX_BEGIN_TIME);
			} catch(Exception e) {
				log.error("[" + coreName + "]" + "�����coreʵ��ʱ�����쳣���޷�����", e);
				return;
			}
			
			
			if(newCore == null) {
				log.error("[" + coreName + "]" + "�޷������coreʵ�����޷�����");
				return;
			}
			
			String indexDir = newCore.getIndexDir();
			File[] dataFiles = (new File(indexDir)).listFiles();
			if(dataFiles == null) {
				log.error("[" + coreName + "]" + "���ȫ�������ļ��б�ʧ�ܣ��޷�����");
				return;
			}
			
			String[] indexFileNames = new String[dataFiles.length];
			
			for(int i=0;i<dataFiles.length;i++) {
				indexFileNames[i] = dataFiles[i].getName();
			}
			
			FullIndexFileProvider provider = new FullIndexFileProvider(new File(indexDir));
			fileServer.register(FullIndexFileProvider.type, provider);
			
			if(!fileServer.isAlive()){
				log.warn("[" + coreName + "]" + " FileGetServerʧЧ,����������Server.");
				try {
					fileServer.start();
				} catch (IOException e) {
					log.error("[" + coreName + "]" + "����FileGetServerʧ��",e);
					return;
				}
			}
			
			
			String masterIp = TerminatorCommonUtils.getLocalHostIP();
			int failedNumeber = 0;
			Set<String> serviceVersions = dumpServices.keySet();
			
			if(serviceVersions == null || serviceVersions.isEmpty()){
				log.warn("[" + coreName + "] �������û����Ӧ��Slave����,�ʲ���Ҫ�����ļ��ĸ���.");
			}
			
			countDownLatch = new CountDownLatch(serviceVersions.size());
			
			for(String serviceVersion : serviceVersions){
				DumpService service = dumpServices.get(serviceVersion);
				boolean isOk = true;
				try{
					isOk = service.notifyToSlave(masterIp, fileServer.getPort(),indexFileNames, beginTime);
					log.warn("[" + coreName + "]" + "Master֪ͨSlave������ȡȫ��������Slave-Version ==> " + serviceVersion + " ֪ͨ ��" + (isOk ? "�ɹ�" : "ʧ��") + "��");
				} catch (Exception e){
					isOk = false;
					log.error("[" + coreName + "]" + "Master֪ͨSlaveʱʧ�� Version ===> " + serviceVersion);
				}
				if(!isOk){
					failedNumeber ++;
				}
			}
			
			if(failedNumeber > 0){
				for(int i=1; i<=failedNumeber;i++){
					countDownLatch.countDown();
				}
			}
			
			try {
				countDownLatch.await(masterWaitTime,TimeUnit.MILLISECONDS);
			} catch (InterruptedException e1) {
				log.error("[" + coreName + "]" + "CountDownLatch�ȴ������г����ж��쳣",e1);
				Thread.currentThread().interrupt();
			}
			
			countDownLatch = null;
			
			solrCore = IndexUtils.swapCores(oldCore, newCore);
			UpdateHandler updateHandler = solrCore.getUpdateHandler();
			if(updateHandler instanceof TerminatorUpdateHandler) {
				((TerminatorUpdateHandler)updateHandler).switchMode(TerminatorUpdateHandler.MODE_REALTIME);
			}
			
			try {
				context.getScheduler().getContext().remove(JOB_CAN_EXECUTE);
				context.getScheduler().getContext().put(DumpService.JOB_SOLR_CORE, solrCore);
			} catch (SchedulerException e) {
				log.error("����ȫ��ִ��״̬ʧ��", e);
				throw new RuntimeException("����ȫ��ִ��״̬ʧ��", e);
			}
			
			IndexFileUtils.writeIncrStartTimeToFile(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome() + File.separatorChar + solrCore.getName() + File.separatorChar, beginTime);
		}
	}
	
	/**
	 * ������������listener
	 */
	private class IncrIndexJobListener implements JobListener {

		@Override
		public String getName() {
			return "IncrIndexJobListener";
		}

		@Override
		public void jobExecutionVetoed(JobExecutionContext context) {
			
		}

		@Override
		public void jobToBeExecuted(JobExecutionContext context) {
			log.warn(solrCore.getName() + " ������������ʼ.");
		}

		@Override
		public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
			log.warn(solrCore.getName() + " ���������������.");
			if(jobException != null){
				log.warn(solrCore.getName() + " �˴�������ʧ�ܸ��� ===>", jobException);
			}
		}
	}
	
	/**
	 * Slaveȥ��Masterȫ���������߳�
	 */
	private class FechFullIndexFilesJob implements Runnable {
		private FetchFileListResponse fileListResponse;
		private String incrDate = null;
		private String msg = null;
		
		public FechFullIndexFilesJob(FetchFileListResponse fileListResponse,String incrDate) {
			this.fileListResponse = fileListResponse;
			this.incrDate = incrDate;
		}
		

		@Override
		public void run() {
			try{
				doRun();
			}finally{
				dumpServices.get(getVersion(fileListResponse.getMasterIp())).reportToMaster(TerminatorCommonUtils.getLocalHostIP(), this.msg);
			}
		}
		
		public void doRun() {
			String masterIp = fileListResponse.getMasterIp();
			int port        = fileListResponse.getPort();
			fileClient = new FileGetClient(masterIp, port);
			List<String> fileNameList = fileListResponse.getFileNameList();
			SolrCore newSolrCore = null;
			
			log.warn("[" + coreName + "]" + "��Mster��������ȫ���������ļ���Maser ==> " + masterIp +":" + port);
			try {
				log.warn("[" + coreName + "]" + "�½�SolrCore,�������µ������ļ�.");
				newSolrCore = IndexUtils.newSolrCore(solrCore);
			} catch (Exception e) {
				log.error("[" + coreName + "]" + "�½�SolrCoreʧ��.",e);
				return;
			}
				
			String dataDirStr = newSolrCore.getDataDir();
			File dataDir = new File(dataDirStr);
			File indexDir = new File(dataDir,"index");
			
			if(indexDir.exists()){
				log.warn("[" + coreName + "]" + "��������ļ�Ŀ¼ ==> " + indexDir.getAbsolutePath());
				try {
					FileUtils.cleanDirectory(indexDir);
				} catch (IOException e1) {
					log.error("[" + coreName + "]" + "��������ļ�Ŀ¼ʧ�� ==> " + indexDir.getAbsolutePath(),e1);
				}
			}else{
				indexDir.mkdirs();
			}
			
			List<String> pendingList = new ArrayList<String>();
			
			log.warn("[" + coreName + "]" + "��Ҫ��ȡ�������ļ��� [" + fileNameList.size() + "] �� ");
			for(String name : fileNameList){
				log.warn("[" + coreName + "]" + "��ȡ�����ļ� ==> " + name);
				File indexFile = new File(indexDir,name);
				FileOutputStream fileOutputStream = null;
				try {
					fileOutputStream = new FileOutputStream(indexFile);
					int code = fileClient.doGetFile(FullIndexFileProvider.type, name, fileOutputStream);
					if(FileGetResponse.SUCCESS != code){
						log.error("[" + coreName + "]" + "��ȡ�ļ�ʧ�ܣ��ļ��� ==> " + name + " error-code :" + code);
						pendingList.add(name);
						if(fileOutputStream != null){
							try {
								fileOutputStream.close();
							} catch (IOException e) {
								log.error(e,e);
							}
						}
						continue;
					}
				} catch (Exception e) {
					log.error("[" + coreName + "]" + "��Master�������ļ�ʧ��  name ==> " + name + "  type ==> fullIndexFiles",e);
					pendingList.add(name);
					if(fileOutputStream != null){
						try {
							fileOutputStream.close();
						} catch (IOException e1) {
							log.error(e,e);
						}
					}
					continue;
				} finally{
					if(fileOutputStream != null){
						try {
							fileOutputStream.close();
						} catch (IOException e) {
							log.error(e,e);
						}
					}
				}
			}

			
			if(pendingList.isEmpty()){
				log.warn("[" + coreName + "]" + "����(Slave)��Master��������ȫ�������ļ��ɹ����л��µ�Core.");
				try {
					indexScheduler.interrupt(solrCore.getName() + INCR_INDEX_JOB_NAME, solrCore.getName() + INDEX_JOB_GROUP);
				} catch (UnableToInterruptJobException e1) {
					log.error("[" + coreName + "]" + "����coreǰ��ֹ����ʱ������ʧ��", e1);
				}
				
				boolean swapSuc = true;
				try{
					synchronized(solrCore){
						solrCore = IndexUtils.swapCores(solrCore, newSolrCore);
						solrCore.getCoreDescriptor().getCoreContainer().persist();
						try {
							indexScheduler.getContext().remove("solrCore");
							indexScheduler.getContext().put("solrCore", solrCore);
						} catch (SchedulerException e) {
							log.error(e,e);
						}
					}
				} catch(Exception e){
					swapSuc = false;
					log.error("[" + coreName + "]" + "�л�Coreʧ��",e);
				} 
				
				if(swapSuc){
					log.warn("[" + coreName + "]" + "Core�л��ɹ�����д������ʼʱ���ļ� ==> " + incrDate);
					IndexFileUtils.writeIncrStartTimeToFile(solrCore.getCoreDescriptor().getCoreContainer().getSolrHome() + File.separatorChar + solrCore.getName() + File.separatorChar, incrDate);
				}
				
				log.warn("[" + coreName + "]" + "����(Slave) ��Master��������ȫ�������ļ���ϣ�����Master���������ܵ���Ϣ.");
				this.msg = "[" + coreName + "]" + " ����ͬ��ȫ�������ļ���ϣ��ɹ�.";
			}else{
				log.warn("[" + coreName + "]" + "����(Slave)��Master��������ȫ�������ļ�ʧ��,����SolrCore���л�,�������ج�ĸ�֪Master�����䲻Ҫ�����ȴ�.ʧ�ܵ��ļ���Ϊ ==> " + pendingList.toArray());
				this.msg = "[" + coreName + "]" + " ����ͬ��ȫ�������ļ�ʧ��!!!.";
			}
		}
	}
	
	public void triggerFullIndexJob() throws SchedulerException {
		log.warn("[" + coreName + "] �ֶ����� [ȫ��] ��������.");
		this.indexScheduler.triggerJob(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
	}
	
	public void triggerIncrIndexJob() throws SchedulerException {
		log.warn("[" + coreName + "] �ֶ����� [����] ��������.");
		this.indexScheduler.triggerJob(this.solrCore.getName() + INCR_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
	}
}