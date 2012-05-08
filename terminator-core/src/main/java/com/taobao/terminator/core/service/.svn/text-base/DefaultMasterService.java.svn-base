package com.taobao.terminator.core.service;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.springframework.scheduling.SchedulingException;

import com.alibaba.common.lang.StringUtil;
import com.taobao.terminator.common.CoreProperties;
import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.TerminatorHsfPubException;
import com.taobao.terminator.common.TerminatorHsfSubException;
import com.taobao.terminator.common.TerminatorMasterServiceException;
import com.taobao.terminator.common.config.GroupConfigSupport;
import com.taobao.terminator.common.config.MastersGroupConfig;
import com.taobao.terminator.common.protocol.FetchFileListResponse;
import com.taobao.terminator.common.protocol.MasterService;
import com.taobao.terminator.common.stream.FileGetServer;
import com.taobao.terminator.common.zk.TerminatorZKException;
import com.taobao.terminator.core.index.FullIndexFileSearcher;
import com.taobao.terminator.core.index.FullIndexWriteJob;
import com.taobao.terminator.core.index.IncrIndexFileSearcher;
import com.taobao.terminator.core.index.IncrIndexWriteJob;
import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.index.OldIncrXmlRemoveJob;
import com.taobao.terminator.core.index.consumer.FullIndexConsumer;
import com.taobao.terminator.core.index.consumer.IncrIndexConsumer;
import com.taobao.terminator.core.index.consumer.IndexConsumer;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.index.stream.IncrXmlFileProvider;
import com.taobao.terminator.core.service.inner.SlaveService;
import com.taobao.terminator.core.util.IndexFileUtils;
import com.taobao.terminator.core.util.IndexUtils;
import com.taobao.terminator.core.util.MasterStatusCollector;
import com.taobao.terminator.core.util.TimeoutHandler;

public class DefaultMasterService implements MasterService,NamedListInitializedPlugin,GroupConfigSupport,TimeoutHandler,Lifecycle{
	private static Log logger = LogFactory.getLog(DefaultMasterService.class);

	private static final String FULL_INDEX_JOB_NAME              = "-FullIndexJob";
	private static final String INCR_INDEX_JOB_NAME              = "-IncrIndexJob";
	private static final String INCR_XML_REMOVE_JOB_NAME         = "-IncrXmlRemoveJob";
	private static final String INDEX_JOB_GROUP                  = "-index";
	private static final String INCR_INDEX_JOB_TRIGGER_NAME      = "-IncrIndexTrigger";
	private static final String JOB_TRIGGER_GROUP                = "-trigger";
	private static final String INCR_XML_REMOVE_JOB_TRIGGER_NAME = "-IncrXmlRemoveTrigger";
	
	private IndexConsumer fullIndexConsumer;
	private IndexConsumer incrIndexConsumer;
	
	private SolrCore solrCore;
	private File fullXmlSourceDir;
	private File incrXmlSourceDir;
	
	private IndexFileSearcher fullIndexFileSearcher;
	
	private IndexFileSearcher incrIndexFileSearcher;
	
	private Scheduler indexScheduler;
	
	private MastersGroupConfig groupConfig = null;
	
	private boolean isMaster = false;
	
	@SuppressWarnings("unchecked")
	private NamedList args = null;
	
	private Set<String>        slaveIps = null;
	private int                fileGetServerPort = 12508;
	private List<SlaveService> slaveServiceList = null;
	private FileGetServer      fileGetServer = null;
	
	private String incrCronExpression          = "0 0/2 * * * ?";
	private String incrXmlRemoveCronExpression = "0 0 0 * * ?";
	
	private String fullDumpStartTime = null;
	
	private SolrCore indexingCore;
	
	public DefaultMasterService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.createXmlDataDir();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args){
		this.args = args;

		MasterStatusCollector.register(solrCore.getName(), masterStatus);

		//初始化是否是Master的信息
		this.initIsMaster();
		if(!isMaster){
			logger.warn("--"+ solrCore.getName() + " 本机器不是Master角色,故不需要初始化Master的服务，亦即不发布相应的Master的HSF服务，也不启动相应的全量的监听线程.");
			return;
		}

		//发布hsf服务
		this.publishHsf();
		
		//订阅hsf服务 ==> 内部的索引源数据的传输服务
		this.subscribeHsf();
		
		//创建两个IndexConsumer
		this.initIndexConsumer();
		
		//初始化两个file searcher
		this.initFileSearcher();
		
		// 初始化增量和全量任务
		try {
			this.initScheduler();
		} catch (SchedulerException e) {
			logger.error("--"+ solrCore.getName() + " 创建Scheduler失败，初始化IndexWriterService失败！", e);
			throw new RuntimeException("--"+ solrCore.getName() + " 初始化IndexWriterService失败！");
		} catch (ParseException e) {
			logger.error("--"+ solrCore.getName() + " 解析增量任务定时模式字符串 失败，初始化IndexWriterService失败！", e);
			throw new RuntimeException("--"+ solrCore.getName() + " 初始化IndexWriterService失败！");
		}		
		
		//启动文件复制监听进程
		this.startFileGetServer();
	}
	
	private void startFileGetServer() {
		String portStr = (String)args.get("fileGetPort");
		if(portStr != null){
			fileGetServerPort = Integer.valueOf(portStr);
		}
		
		logger.warn("启动FileGetServer,host ==> 127.0.0.1  port ==> " + fileGetServerPort);
		fileGetServer = new FileGetServer(TerminatorCommonUtils.getLocalHostIP(), fileGetServerPort);
		
		//增量的文件夹不会发生变化，但是索引的目录文件夹时会随着全量的进行而发生变化的，故需要动态的注册
		IncrXmlFileProvider increFileProvider = new IncrXmlFileProvider(incrXmlSourceDir);
		fileGetServer.register(IncrXmlFileProvider.type, increFileProvider);
		
		try {
			if(!fileGetServer.isAlive()){
				fileGetServer.start();
				this.fileGetServerPort = fileGetServer.getPort();
				logger.warn("FileGetServer的端口为 ==> " + this.fileGetServerPort);
			}
		} catch (IOException e) {
			logger.error("启动FileGetServer失败",e);
			throw new RuntimeException("启动FilGetServer失败",e);
		}
	}
	/**
	 * 注册索引文件的FileProvider,由于此文件路径会随着全量的进行进行变化，故每次全量任务跑完之后均需要调用此方法<br>
	 * PS.全量任务跑完后  Slave开始拖文件之前 调用此方法
	 */
	private void registFullIndexFileProvider(){
		String indexDir = this.indexingCore.getIndexDir();
		FullIndexFileProvider provider = new FullIndexFileProvider(new File(indexDir));
		this.fileGetServer.register(FullIndexFileProvider.type, provider);
	}

	private void initScheduler() throws SchedulerException, ParseException{
		DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
		RAMJobStore jobStore = new RAMJobStore();
		SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
		jobStore.setMisfireThreshold(60000);
		schedulerFactory.createScheduler(this.solrCore.getName() + "-scheduler", 
				this.solrCore.getName() + "-scheulerInstance", threadPool, jobStore);
		threadPool.initialize();
		this.indexScheduler = schedulerFactory.getScheduler(this.solrCore.getName() + "-scheduler");
		
		//全量消费任务，每次接收到全量请求时由MasterService主动触发
		JobDetail fullIndexJobDetail = new JobDetail(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, FullIndexWriteJob.class);
		fullIndexJobDetail.setDurability(true);
		//增量消费任务，定时触发
		JobDetail incrIndexJobDetail = new JobDetail(this.solrCore.getName() + INCR_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, IncrIndexWriteJob.class);
		
		//增量xml清理任务，每天出发一次
		JobDetail incrXmlRemoveJob = new JobDetail(this.solrCore.getName() + INCR_XML_REMOVE_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, OldIncrXmlRemoveJob.class);
		
		String s = (String)this.args.get("incrCronExpression");
		if(s != null && StringUtil.isNotBlank(s)){
			this.incrCronExpression = s;
		}
		s = (String)this.args.get("incrXmlRemoveCronExpression");
		if(s != null && StringUtil.isNotBlank(s)){
			this.incrXmlRemoveCronExpression = s;
		}
		
		CronTrigger incrJobTrigger = new CronTrigger(this.solrCore.getName() + INCR_INDEX_JOB_TRIGGER_NAME, this.solrCore.getName() + JOB_TRIGGER_GROUP, this.incrCronExpression);
		CronTrigger xmlRemoveTrigger = new CronTrigger(this.solrCore.getName() + INCR_XML_REMOVE_JOB_TRIGGER_NAME, this.solrCore.getName() + JOB_TRIGGER_GROUP, this.incrXmlRemoveCronExpression);
		
		this.indexScheduler.getContext().put("solrCore", this.solrCore);
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-fullIndexFileSearcher", this.fullIndexFileSearcher);
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-incrIndexFileSearcher", this.incrIndexFileSearcher);
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-incrXmlRootDir", this.incrXmlSourceDir.getAbsolutePath());
		
		this.indexScheduler.addJobListener(new FullIndexJobListener());
		this.indexScheduler.addJobListener(new MasterIncrIndexJobListener());
		
		fullIndexJobDetail.addJobListener("FullIndexJobListener");
		incrIndexJobDetail.addJobListener("MasterIncrIndexJobListener");
		
		this.indexScheduler.addJob(fullIndexJobDetail, true);
		this.start();
			
		this.indexScheduler.scheduleJob(incrIndexJobDetail, incrJobTrigger);	
		this.indexScheduler.scheduleJob(incrXmlRemoveJob, xmlRemoveTrigger);
	}
	
	
	private TimeoutThread notifySlaveTimeoutThread = null;
	
	private class FullIndexJobListener implements JobListener{

		@Override
		public String getName() {
			return "FullIndexJobListener";
		}

		@Override
		public void jobExecutionVetoed(JobExecutionContext context) {/* nothing to do*/}

		@Override
		public void jobToBeExecuted(JobExecutionContext context) {
			if(slaveServiceList != null && !slaveServiceList.isEmpty()){
				logger.warn("[Full-Index]--" + solrCore.getName() + " 全量索引任务即将开始执行，将这个状态告知此组的所有Slave机器，以便其作出相应动作.");
				for(SlaveService slaveService : slaveServiceList){
					slaveService.notifySlaveAfterStartFull();
				}
			}
			
			try {
				fullDumpStartTime = IndexFileUtils.getIncreStartDate(incrXmlSourceDir);
			} catch (IOException e) {
				logger.warn("[Full-Index]--"+ solrCore.getName() + " 获取增量开始时间失败,以当前时间替代.");
				fullDumpStartTime = IndexFileUtils.formatDate(new Date());
			}
			logger.warn("[Full-Index]--"+ solrCore.getName() + " 全量开始的时间为 ==> " + fullDumpStartTime + " 全量结束后会用此事件替代增量开始文件中的时间，用以订正全量时候的增量数据.");
		}

		@Override
		public void jobWasExecuted(JobExecutionContext jobContext,JobExecutionException exception) {
			boolean slaveNeedFetchIndex = exception == null;
			boolean fullIndexisOk       = slaveNeedFetchIndex;
			
			logger.warn("[Full-Index] --"+ solrCore.getName() + " 全量索引dump任务结束  ==> " +(fullIndexisOk ? "[正常]":"[异常]") + "结束.");
			
			indexingCore = (SolrCore)jobContext.get("newSolrCore");
			registFullIndexFileProvider();
			
			if(!fileGetServer.isAlive()){
				logger.warn("--"+ solrCore.getName() + " FileGetServer失效,重新启动该Server.");
				try {
					fileGetServer.start();
				} catch (IOException e) {
					logger.error("启动FileGetServer失败",e);
				}
			}
			
			if(slaveServiceList != null && !slaveServiceList.isEmpty()){ //有Slave机器
				logger.warn("--"+ solrCore.getName() + " 有Slave机器，开始一一通知Slave机器来拖全量好的索引.");
				if(slaveNeedFetchIndex){ //Master正常结束
					String indexDirStr = indexingCore.getIndexDir();
					File indexDir = new File(indexDirStr);
					File[] files = indexDir.listFiles();
					List<String> fileNameList = new ArrayList<String>(files.length);
					
					if(files == null || files.length == 0){
						logger.error(" --"+ solrCore.getName() + " ********* 索引目录  " + indexDirStr + "下没有索引文件,此次全量有问题!");
						return;
					}
					
					for(File file : files){
						fileNameList.add(file.getName());
					}
					
					FetchFileListResponse fileListResponse = new FetchFileListResponse(TerminatorCommonUtils.getLocalHostIP(),fileGetServerPort,fileNameList);
					
					{
						//启动超时监听线程
						logger.warn("--"+ solrCore.getName() + "启动NotifySlaveTimeoutThread超时监听线程.");
						notifySlaveTimeoutThread = new NotifySlaveTimeoutThread(30 * 60 * 1000); //30分钟
						notifySlaveTimeoutThread.setName("Notify-Slave-TimeOut-Listen-Thread");
						notifySlaveTimeoutThread.start();
					}
					for(SlaveService slaveService : slaveServiceList){
						try{
							slaveService.notifySlaveAfterFull(true, fileListResponse, fullDumpStartTime);
						}catch(Exception e){
							logger.error("全量结束后通知Slave失败(SlaveSerivce.notifySlaveAfterFull)",e);
						}
						masterStatus.slaveCount.incrementAndGet();
					}
				}else{//Master全量有问题，告知Slave不需要同步索引文件，Slave只需要设置相应的标志位
					logger.warn("--"+ solrCore.getName() + "Master的全量异常结束，将这个噩耗一一告知Slave吧.");
					for(SlaveService slaveService : slaveServiceList){
						try{
							slaveService.notifySlaveAfterFull(false, null, null);
						}catch(Exception e){
							logger.error("--"+ solrCore.getName() + " 全量结束后通知Slave失败(SlaveSerivce.notifySlaveAfterFull)",e);
						}
					}
					
					logger.warn("--"+ solrCore.getName() + " 整个全量索引的构建全部结束了，状态为全部归位。");
					//重写增量开始时间文件
					IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
					masterStatus.reset();
				}
			}else{//没有Slave机器
				masterStatus.slaveCount.set(0);
				if(fullIndexisOk){
					try{
						logger.warn("--"+ solrCore.getName() + " 该组机器没有Slave角色的机器,故直接交换Core.");
						swapCore();
						masterStatus.indexing.set(false);
					}catch(Exception e){
						logger.error("--"+ solrCore.getName() + " 全量完毕后交换Core失败,所以依然使用原来的Core对外提供服务.",e);
					}finally{
						//重写增量开始时间文件
						IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
					}
				}else{
					logger.warn("--"+ solrCore.getName() + " 此次全量Dump有问题，故不交换Core.");
				}
				masterStatus.reset();
			}
		}
		
	}
	
	private class MasterIncrIndexJobListener implements JobListener{

		@Override
		public String getName() {
			return "MasterIncrIndexJobListener";
		}

		@Override
		public void jobExecutionVetoed(JobExecutionContext context) {/* nothing to do */}

		@Override
		public void jobToBeExecuted(JobExecutionContext context) {
			logger.warn("--"+ solrCore.getName() + " 一次增量消费开始。");
			context.put("isFullIndexing", new Boolean(masterStatus.indexing.get()));
		}

		@Override
		public void jobWasExecuted(JobExecutionContext context,JobExecutionException exception) {
			logger.warn("--"+ solrCore.getName() + " 一次增量消费任务结束。");
			if(exception != null){
				logger.warn("--"+ solrCore.getName() + " 此次增量以失败告终 ===>",exception);
			}
		}
	}
	
	private void initIsMaster(){
		String coreName = this.solrCore.getName();
		CoreProperties coreProperties = null;
		try {
			coreProperties = new CoreProperties(solrCore.getResourceLoader().openConfig("core.properties"));
		} catch (IOException e) {
			logger.warn("--"+ solrCore.getName() + " 读取 [" + coreName +"] 的core.properties文件失败",e);
			return;
		}
		
		this.isMaster = coreProperties != null && coreProperties.isWriter();
	}

	/**
	 * Master需要订阅Slave发布的服务
	 */
	private void  subscribeHsf(){
		String coreName = solrCore.getName();
		logger.warn("本机器对应于  " + coreName + "  是Master角色,故需要订阅相对应的Slave发布的内部通讯服务.");
		String[] ss = TerminatorCommonUtils.splitCoreName(coreName);
		if(ss == null){
			logger.error("CoreName ==> " + coreName + " 分解成serviceName groupName失败.");
		}
		
		logger.warn("从ZooKeeper上获取与 " + coreName + "对应的GroupConfig信息.");
		try {
			groupConfig = new MastersGroupConfig(ss[0], ss[1], ZkClientHolder.zkClient, this);
		} catch (TerminatorZKException e) { 
			logger.error(e,e);
		}
		this.onGroupConfigChange(groupConfig);
	}
	
	@Override
	public void onGroupConfigChange(MastersGroupConfig groupConfig) {
		String coreName = this.solrCore.getName();
		logger.warn("SolrCore ==> " + coreName + "对应的Slave机器发生了变化或者这是启动阶段，(重新)订阅Slave发布的内部通讯服务.");
		
		if(slaveServiceList == null){
			slaveServiceList = new ArrayList<SlaveService>();
		}else{
			slaveServiceList.clear();
		}
		
		this.groupConfig = groupConfig;
		if(slaveIps == null){
			slaveIps = new HashSet<String>();
		}
		Set<String> allIps = this.groupConfig.keySet();
		String localIp = TerminatorCommonUtils.getLocalHostIP();

		for(String ip : allIps){
			if(localIp.equals(ip)){
				continue;
			}
			slaveIps.add(ip);
			String hsfVersion = TerminatorHSFContainer.Utils.generateSlaveWriteService(coreName, ip);
			logger.warn("订阅  HsfVerion ==> " + hsfVersion);
			try {
				SlaveService slaveService = (SlaveService)TerminatorHSFContainer.subscribeService(SlaveService.class.getName(), hsfVersion).getObject();
				slaveServiceList.add(slaveService);
			} catch (TerminatorHsfSubException e) {
				logger.error(e,e);
			} catch (Exception e) {
				logger.error(e,e);
			}
		}
	}
	
	private void publishHsf(){
		String coreName = this.solrCore.getName();
		logger.warn("发布对应于  " + coreName +"  的HSF服务.");
		String hsfVersion = TerminatorHSFContainer.Utils.genearteVersion(ServiceType.writer, coreName);
		logger.warn("本机器对应于  " + coreName + "  是Master角色，故须对终搜客户端对外暴露写索引服务.");
		logger.warn("发布  HsfVerison ==> " + hsfVersion);
		try {
			TerminatorHSFContainer.publishService(this, MasterService.class.getName(), hsfVersion,10 * 1000); //超时时间设置长一些 
		} catch (TerminatorHsfPubException e) {
			logger.error(e,e);
		}
	}
	
	private void initFileSearcher(){
		this.fullIndexFileSearcher = new FullIndexFileSearcher(this.fullXmlSourceDir);
		this.incrIndexFileSearcher = new IncrIndexFileSearcher(this.incrXmlSourceDir);
	}
	
	private void initIndexConsumer(){
		this.fullIndexConsumer = new FullIndexConsumer(this.fullXmlSourceDir, 5);
		this.incrIndexConsumer = new IncrIndexConsumer(this.incrXmlSourceDir, 5);
	}
	/**
	 * 初始化存放xml数据的目录，保存在solr home目录下面，在做core切换的时候不需要更改。
	 */
	private void createXmlDataDir(){
		File fullDir = new File(this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()+ File.separator + this.solrCore.getName() + File.separator + "full_xml_source");
		File incrDir = new File(this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()+ File.separator + this.solrCore.getName() + File.separator + "incr_xml_source");
		
		if(!fullDir.exists()){
			fullDir.mkdir();
		}
		if(!incrDir.exists()){
			incrDir.mkdir();
		}
		
		this.fullXmlSourceDir = fullDir;
		this.incrXmlSourceDir = incrDir;
	}
	
	public void triggerFullIndex() throws TerminatorMasterServiceException{
		logger.warn("--"+ solrCore.getName() + "[Trigger-Full-Index-Job] 触发全量Index任务.....");
		try {
			IndexUtils.cleanDir(this.fullXmlSourceDir);
		} catch (IOException e) {
			logger.warn("--"+ solrCore.getName() + "清理全量xml目录失败,path ==> " + this.fullXmlSourceDir.getName(), e);
		} 
		
		try {
			this.indexScheduler.triggerJob(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
		} catch (SchedulerException e) {
			logger.error("--"+ solrCore.getName() + "初始化任务Scheduler失败。。", e);
			throw new TerminatorMasterServiceException("--"+ solrCore.getName() + "初始化任务Scheduler失败。。");
		}
	}
	
	private MasterStatus masterStatus = new MasterStatus();
	private TimeoutThread fullDumpTimeoutThread = null;
	
	public boolean startFullDump(String clientIp) throws TerminatorMasterServiceException{
		logger.warn("["+ solrCore.getName() + "] 调用startFullDump方法，当前Master的状态为 ==> " + masterStatus.toString());
		if(masterStatus.remoteClients.contains(clientIp)){
			logger.error("[" + solrCore.getName() +"] IP为 " +clientIp + " 的客户端已经调用过startFullDump方法了,故此次调用无效.");
			return false;
		}else{
			masterStatus.remoteClients.add(clientIp);
			logger.warn("[" + solrCore.getName() + "] 已经调用startFull方法的客户端有 ==> " + masterStatus.remoteClients);
		}
		
		if(masterStatus.remoteClienCount.incrementAndGet() == 1){
			{
				logger.warn("--"+ solrCore.getName() + " 启动FullDumpTimeoutThread超时监听线程,超时时间为 :" + 12 * 60 * 60 * 1000 + "ms,如果在该时间内数据没有传送完毕，则接下来传送多来的数据将被丢弃.");
				fullDumpTimeoutThread = new FullDumpTimeoutThread(12 * 60 * 60 * 1000);
				fullDumpTimeoutThread.setName("Full-Dump-TimeOut-Listen-Thread");
				fullDumpTimeoutThread.start();
			}
			
			fullIndexConsumer.start();
			triggerFullIndex();
			masterStatus.started.set(true);
			masterStatus.indexing.set(true);
		}
		
		return true;
	}

	public boolean fullDump(String clientIp,byte[] indexData) throws TerminatorMasterServiceException{
		logger.warn("--"+ solrCore.getName() + " 调用fullDump方法,当前Master的状态为 ==> " + masterStatus.toString());
		if((masterStatus.started.get() || masterStatus.dumping.get()) && !masterStatus.finished.get() && masterStatus.remoteClients.contains(clientIp)){
			masterStatus.dumping.set(true);
			fullIndexConsumer.consum(indexData);
			return true;
		}else{
			logger.warn("--"+ solrCore.getName() + " [Dump]拒绝本次全量请求,调用该方法的客户端IP为 : + " +	clientIp + "当前状态为 ==> " + masterStatus.toString());
			return false;
		}
	}
	
	public boolean finishFullDump(String clientIp) {
		logger.warn("--"+ solrCore.getName() + " 调用finishFullDump方法,当前Master的状态为 ==> " + masterStatus.toString());
		
		if(masterStatus.remoteClients.contains(clientIp)){
			masterStatus.remoteClients.remove(clientIp);
		}else{
			logger.warn("[" + solrCore.getName() + "] IP为  " + clientIp +" 的客户端机器没有调用过startFull方法，当前调合法的ClientIps是:" + masterStatus.remoteClients);
			return false;
		}
		
		if(masterStatus.started.get() && !masterStatus.finished.get() ){
			if(masterStatus.remoteClienCount.decrementAndGet() == 0){
				fullIndexConsumer.finish();	
				fullDumpTimeoutThread.finish();
				masterStatus.finished.set(true);
			} else{	
				logger.warn("["+ solrCore.getName() + "]  客户端调用了一次全量索引xml数据传送结束请求，当前还有[" + this.masterStatus.remoteClienCount.get() + "]个客户端正在传送数据。");
			}
			return true;
		}else{
			logger.error("["+ solrCore.getName() + "] 拒绝本次请求(finishFullDump),当前状态为 ==> " + masterStatus.toString());
			return false;
		}
	}
	
	/**
	 * 全量Dump超时监听线程
	 * 
	 * @author yusen
	 */
	private abstract class TimeoutThread extends  Thread{
		private long   timeout   = 0L;
		private long   startTime = 0L;
		private boolean runnable = true;
		
		TimeoutThread(long timeout){
			this.timeout   = timeout;
			this.startTime = System.currentTimeMillis();
			this.runnable  = true;
		}
		
		@Override
		public void run() {
			logger.warn("[Time-Out-Thread] 开启超时线程，TIME_OUT=[" + timeout + " ms]");
			while(runnable){
				try {
					Thread.sleep(60 * 1000);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
				if(System.currentTimeMillis() - startTime >= timeout){
					this.handleTimeoutEvent();
					return;
				}
			}
		}
		
		public void finish(){
			logger.warn("[Time-Out-Thread] 结束超时线程.");
			this.runnable = false;
		}
		
		public abstract void handleTimeoutEvent();
	}
	
	private class NotifySlaveTimeoutThread extends TimeoutThread{
		
		NotifySlaveTimeoutThread(long timeout) {
			super(timeout);
		}

		@Override
		public void handleTimeoutEvent() {
			logger.warn("处理NotifySlaveTimeoutThread的超时事件.");
			masterStatus.slaveCount.set(1);
			try {
				pullIndexFinished("Slave没有在Timeout时间内复制完毕.啥都不管了，Master该干啥干啥.");
			} catch (TerminatorMasterServiceException e) {
				logger.error(e,e);
			}
		}
	}
	
	private class FullDumpTimeoutThread extends TimeoutThread{

		FullDumpTimeoutThread(long timeout) {
			super(timeout);
		}

		@Override
		public void handleTimeoutEvent() {
			logger.warn("[Time-Out-Thread] 全量Dump数据的超时事件.");
			fullIndexConsumer.finish();	
			masterStatus.finished.set(true);
			masterStatus.remoteClienCount.set(0);
		
		}
	}

	public boolean startIncDump() throws TerminatorMasterServiceException{
		if(this.incrIndexConsumer == null){
			logger.warn("-----------------------------------incrIndexConsumer is null ------------------------------");
		} else{
			logger.warn("-----------------------------------incrIndexConsumer is ok ------------------------------");
		}
		this.incrIndexConsumer.start();
		return true;
	} 
	
	public boolean incrDump(byte[] indexData) throws TerminatorMasterServiceException{		
		this.incrIndexConsumer.consum(indexData);
		return true;
	}
	
	public boolean finishIncrDump() throws TerminatorMasterServiceException{
		this.incrIndexConsumer.finish();
		return true;
	}

	public MastersGroupConfig getGroupConfig() {
		return groupConfig;
	}
	
	/**
	 * slave调用这个方法来通知master自己已经拖完了全量索引数据了
	 */
	public void pullIndexFinished(String msg) throws TerminatorMasterServiceException{
		logger.warn("--"+ solrCore.getName() + " Slave向Master报告同步全量索引的情况 ==> " + msg);
		
		if(this.masterStatus.slaveCount.decrementAndGet() == 0){
			logger.warn("--"+ solrCore.getName() + " slave已经都拖完了全量索引，开始交换SolrCore");
			try{
				this.swapCore();
				this.solrCore.getCoreDescriptor().getCoreContainer().persist();
				IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
				notifySlaveTimeoutThread.finish();
				this.masterStatus.indexing.set(false);
			}catch(Exception e){
				logger.error("--"+ solrCore.getName() + "全量完毕后交换Core失败.",e);
			} finally{
				this.masterStatus.reset();
			}
		}
	}
	
	private void swapCore(){
		if(this.indexingCore != null){
			synchronized(this.solrCore){			
				this.solrCore = IndexUtils.swapCores(this.solrCore, this.indexingCore);
				try {
					this.indexScheduler.getContext().remove("solrCore");
					this.indexScheduler.getContext().put("solrCore", this.solrCore);
				} catch (SchedulerException e) {
					logger.error("--"+ solrCore.getName() + "获取SchedulerContext失败，core的交换成功，但是SchedulerContext中的老的SolrCore", e);
				}
			}
			this.indexingCore = null;
		}
	}
	
	/**
	 * 返回Date时间点以后的所有文件的文件名列表
	 */
	public FetchFileListResponse fetchIncrFileList(final String date){
		logger.warn("--"+ solrCore.getName() + " Slave取增量xml文件 ,起始时间为 ==> " + date);
		Collection<File> fileList = new LinkedList<File>();
		try {
			fileList = IndexFileUtils.listIncrFile(this.incrXmlSourceDir, date, 100);
		} catch (ParseException e) {
			logger.error("--"+ solrCore.getName() + " 输入的参数格式错误，date:" + date, e);
			return new FetchFileListResponse(TerminatorCommonUtils.getLocalHostIP(),fileGetServerPort,null);
		}
		List<String> fileNameList = new ArrayList<String>(fileList.size());
		for(File file : fileList){
			fileNameList.add(file.getName());
		}
		return new FetchFileListResponse(TerminatorCommonUtils.getLocalHostIP(),fileGetServerPort,fileNameList);
	}
	
	public String getName() {
		return "FullIndexWriteJobListener";
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

	public void stop() throws SchedulingException {
		if (this.indexScheduler != null) {
			try {
				this.indexScheduler.standby();
			}
			catch (SchedulerException ex) {
				throw new SchedulingException("Could not stop Quartz Scheduler", ex);
			}
		}
	}

	public boolean isRunning() throws SchedulingException {
		if (this.indexScheduler != null) {
			try {
				return !this.indexScheduler.isInStandbyMode();
			}
			catch (SchedulerException ex) {
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean isIncrIndexRunning() throws TerminatorMasterServiceException {
		throw new TerminatorMasterServiceException(new UnsupportedOperationException("isIncrIndexRunning()"));
	}

	@Override
	public boolean isIndexRunning() throws TerminatorMasterServiceException {
		throw new TerminatorMasterServiceException(new UnsupportedOperationException("isIncrIndexRunning()"));
	}
	
	@Override
	public boolean isFullIndexRunning() throws TerminatorMasterServiceException{
		throw new TerminatorMasterServiceException(new UnsupportedOperationException("isIncrIndexRunning()"));
	}
}
