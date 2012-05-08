/*package com.taobao.terminator.core.service;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.taobao.terminator.common.TerminatorIndexWriteException;
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
import com.taobao.terminator.core.index.IndexContext;
import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.index.OldIncrXmlRemoveJob;
import com.taobao.terminator.core.index.consumer.FullIndexConsumer;
import com.taobao.terminator.core.index.consumer.IncrIndexConsumer;
import com.taobao.terminator.core.index.consumer.IndexConsumer;
import com.taobao.terminator.core.index.importer.InstantSolrXMLDataImporter;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.index.stream.IncrXmlFileProvider;
import com.taobao.terminator.core.service.inner.SlaveService;
import com.taobao.terminator.core.util.IndexFileUtils;
import com.taobao.terminator.core.util.IndexUtils;
import com.taobao.terminator.core.util.TimeoutHandler;
import com.taobao.terminator.core.util.TimeoutThread;

public class OldMasterService implements 
	MasterService,NamedListInitializedPlugin,
	GroupConfigSupport,JobListener,TimeoutHandler,Lifecycle{
	

	private static final long SLAVE_TIMEOUT_THRESHOLD = 60 * 1000 * 10;
	private static final long CLIENT_TIMEOUT_THRESHOLD = 60 * 1000 * 10;
	private static final String FULL_INDEX_JOB_NAME = "-FullIndexJob";
	private static final String INCR_INDEX_JOB_NAME = "-IncrIndexJob";
	private static final String INCR_XML_REMOVE_JOB_NAME = "-IncrXmlRemoveJob";
	private static final String INDEX_JOB_GROUP = "-index";

	private static final String INCR_INDEX_JOB_TRIGGER_NAME = "-IncrIndexTrigger";
	private static final String INCR_XML_REMOVE_JOB_TRIGGER_NAME = "-IncrXmlRemoveTrigger";
	
	private static final String JOB_TRIGGER_GROUP = "-trigger";
	private static final String INCR_INDEX_JOB_CYCLE = "0 0/2 * * * ?"; //每两分钟出发一次增量消费
	private static final String INCR_XML_REMOVE_JOB_CYCLE = "0 0 0 * * ?"; //每天0时出发一次增量xml清理任务

	private static final int    INSTANT_INDEX_JOB_NUM       = 0x0005;

	
	private static Log logger = LogFactory.getLog(OldMasterService.class);
	
	private IndexConsumer fullIndexConsumer;
	private IndexConsumer incrIndexConsumer;
	
	private ThreadPoolExecutor instantIndexJobs;
	
	*//**
	 * 当前正在使用core
	 *//*
	private SolrCore solrCore;
	
	*//**
	 * 在进行全量索引时，构建索引用的xml数据被写入的文件夹，data import从这个文件夹中读取xml文件来构建索引
	 *//*
	private File fullXmlSourceDir;
	
	*//**
	 * 在进行增量索引时，构建索引用的xml数据被写入的文件夹，data import从这个文件夹中读取xml文件来构建索引
	 *//*
	private File incrXmlSourceDir;
	
	*//**
	 * 记录被调用几次startFullDump方法
	 *//*
	private AtomicInteger startFullDumpCount;
	
	private IndexFileSearcher fullIndexFileSearcher;
	
	private IndexFileSearcher incrIndexFileSearcher;
	
	private Scheduler indexScheduler;
	
	private MastersGroupConfig groupConfig = null;
	
	private boolean isMaster = false;
	
	private Set<String> slaveIps = null;

	*//**
	 * 用来记录正在拖数据的salve的个数
	 *//*
	private AtomicInteger slavePullCount;
	
	@SuppressWarnings("unchecked")
	private NamedList args = null;
	
	private int fileGetServerPort = 12508;
	
	private List<SlaveService> slaveServiceList = null;
	
	private FileGetServer fileGetServer = null;
	
	private String incrCronExpression = INCR_INDEX_JOB_CYCLE;
	
	private String incrXmlRemoveCronExpression = INCR_XML_REMOVE_JOB_CYCLE;
	
	*//**
	 * 当客户端发起一次全量请求的时候，会开启一个超时线程
	 * 每当客户端调用了一次fullDump方法，就会令当前的这个
	 * 超时线程中断，然后重新开一个超时线程。如果客户端在
	 * 调用了一次fullDump之后，在超时时间内没有再调用fullDump
	 * 或者fullDumpFinish之一的方法，那么就会触发超时处理操作。
	 *//*
	private TimeoutThread clientInvokeTimeoutThread;
	
	*//**
	 * 如果slave在超时时间之内没有调用pullIndexFinished方法
	 * 那么就会触发超时处理操作。每次slave调用pullIndexFinished
	 * 方法 就会中断当前的超时线程，并且重新开启一个。
	 *//*
	private TimeoutThread slaveInvokeTimeoutThread;
	
	*//**
	 * 引用全量索引结束，solrCore切换之前，被创建的用来
	 * 导入全量索引的那个solrCore。这个core由FullIndexWriteJob创建。
	 * 当FullIndexWriteJob结束的时候通过JobListener接口传回来。
	 *//*
	private SolrCore indexingCore;
	
	public OldMasterService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.createXmlDataDir();
		this.instantIndexJobs = new ThreadPoolExecutor(INSTANT_INDEX_JOB_NUM, INSTANT_INDEX_JOB_NUM, Long.MAX_VALUE, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
		this.startFullDumpCount = new AtomicInteger(0);
		this.slavePullCount = new AtomicInteger(0);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args){
		this.args = args;

		//初始化是否是Master的信息
		this.initIsMaster();
		if(!isMaster){
			logger.warn("本机器不是Master角色,故不需要初始化Master的服务，亦即不发布相应的Master的HSF服务，也不启动相应的全量的监听线程.");
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
			logger.error("创建Scheduler失败，初始化IndexWriterService失败！", e);
			throw new RuntimeException("初始化IndexWriterService失败！");
		} catch (ParseException e) {
			logger.error("解析增量任务定时模式字符串 失败，初始化IndexWriterService失败！", e);
			throw new RuntimeException("初始化IndexWriterService失败！");
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
	*//**
	 * 注册索引文件的FileProvider,由于此文件路径会随着全量的进行进行变化，故每次全量任务跑完之后均需要调用此方法<br>
	 * PS.全量任务跑完后  Slave开始拖文件之前 调用此方法
	 *//*
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
		
		this.indexScheduler.addJobListener(this);
		
		fullIndexJobDetail.addJobListener("FullIndexWriteJobListener");
		
		this.indexScheduler.addJob(fullIndexJobDetail, true);
		
		this.start();
			
		this.indexScheduler.scheduleJob(incrIndexJobDetail, incrJobTrigger);	
		this.indexScheduler.scheduleJob(incrXmlRemoveJob, xmlRemoveTrigger);
	}
	
	private void initIsMaster(){
		String coreName = this.solrCore.getName();
		CoreProperties coreProperties = null;
		try {
			coreProperties = new CoreProperties(solrCore.getResourceLoader().openConfig("core.properties"));
		} catch (IOException e) {
			logger.warn("读取 [" + coreName +"] 的core.properties文件失败",e);
			return;
		}
		
		this.isMaster = coreProperties != null && coreProperties.isWriter();
	}

	*//**
	 * Master需要订阅Slave发布的服务
	 *//*
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
	
	//by yusen
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
		this.fullIndexConsumer = new FullIndexConsumer(this.fullXmlSourceDir, 1);
		this.incrIndexConsumer = new IncrIndexConsumer(this.incrXmlSourceDir, 1);
	}
	*//**
	 * 初始化存放xml数据的目录，保存在solr home目录下面，在做core切换的时候不需要更改。
	 *//*
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
	
	public void finishFullDump() {
		if(this.clientInvokeTimeoutThread == null){
			logger.warn("客户端全量索引超时线程为null，说明没有调用过startFulLDump方法，拒绝本次请求。");
			return;
		}
		//全量计数器-1，当等于0的时候开始结束操作
		if(this.startFullDumpCount.decrementAndGet() == 0){
			logger.warn("所有的客户端都已经通知数据传送完成了，结束IndexConsumer。");
			this.fullIndexConsumer.finish();		
			//调用在超时时间之内正常结束，让客户端超时监听线程正常结束
			this.clientInvokeTimeoutThread.stopRunning();
			this.clientInvokeTimeoutThread.interrupt();
			this.clientInvokeTimeoutThread = null;
		} else{	
			//让客户端超时监听线程重新开始一次监听过程
			this.clientInvokeTimeoutThread.sleepAgain();
			this.clientInvokeTimeoutThread.interrupt();
			logger.warn("客户端调用了一次全量索引xml数据传送结束请求，" + "但是还有其他的客户端没有调用，当前还有<" + this.startFullDumpCount.get() + ">个客户端正在传送数据。");
		}
	}
	
	public void incrDumpFinish() throws TerminatorIndexWriteException{
		this.incrIndexConsumer.finish();
		IndexContext.isIncrIndexing.set(false);
	}

	public void fullDump(byte[] indexData) throws TerminatorIndexWriteException{
		if(!IndexContext.isDataTransmitFinish.get()){
			this.clientInvokeTimeoutThread.sleepAgain();
			this.clientInvokeTimeoutThread.interrupt();
			this.fullIndexConsumer.consum(indexData,null);
		}else{
			logger.warn("丢弃数据...");
		}
	}
	
	public void incrDump(byte[] indexData) throws TerminatorIndexWriteException{		
		this.incrIndexConsumer.consum(indexData,null);
	}
	
	public void instantDump(byte[] indexData) throws TerminatorIndexWriteException{
		if(IndexContext.isFullIndexing.get() || IndexContext.isIncrIndexing.get()){
			logger.warn("当前有全量或者增量索引正在执行，拒绝本次实时索引写入请求");
			return;
		} else{
			InstantSolrXMLDataImporter indexImporter =  new InstantSolrXMLDataImporter(this.solrCore, indexData);
			this.instantIndexJobs.execute(indexImporter);
		}
	}

	public boolean startFullDump() throws TerminatorIndexWriteException{
		this.startFullDumpCount.incrementAndGet();
		if(this.startFullDumpCount.get() == 1 && IndexContext.isFullIndexing.get()){
			logger.warn("上一次索引还没有完成，拒绝本次索引请求！");
			//将startFullDumpCount减1
			this.startFullDumpCount.decrementAndGet();
			return false;
		}
		
		if(this.startFullDumpCount.get() == 1 && !IndexContext.isFullIndexing.get()){
			logger.warn("客户端调用了一次全量索引请求，这是第一次，全量索引开始 ---- 触发全量任务.");
			IndexContext.isFullIndexing.set(true);
			try {
				IndexUtils.cleanDir(this.fullXmlSourceDir);
			} catch (IOException e) {
				logger.warn("清理全量xml目录失败。。dir:" + this.fullXmlSourceDir.getName(), e);
			} 
			
			try {
				this.fullIndexConsumer.start();
				this.indexScheduler.triggerJob(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
			} catch (SchedulerException e) {
				logger.error("初始化任务Scheduler失败。。", e);
				throw new TerminatorIndexWriteException("初始化任务Scheduler失败。。");
			}
			
			this.clientInvokeTimeoutThread = new TimeoutThread(CLIENT_TIMEOUT_THRESHOLD, this, "handlerClientFullDumpTimeout");
			this.clientInvokeTimeoutThread.start();
		} else{
			logger.warn("客户端调用了一次全量索引请求，但是之前已经有客户端调用过来，这是第<"  + this.startFullDumpCount.get() + ">次请求");
		}
		return true;
	}

	public boolean startIncDump() throws TerminatorIndexWriteException{
		IndexContext.isIncrIndexing.set(true);
		this.incrIndexConsumer.start();
		return true;
	} 
	
	public boolean isFullIndexRunning() throws TerminatorIndexWriteException{
		return IndexContext.isFullIndexing.get();
	}

	public boolean isIncrIndexRunning() throws TerminatorIndexWriteException{
		return IndexContext.isIncrIndexing.get();
	}

	public boolean isIndexRunning() throws TerminatorIndexWriteException{
		return (IndexContext.isFullIndexing.get() || IndexContext.isIncrIndexing.get());
	}

	public MastersGroupConfig getGroupConfig() {
		return groupConfig;
	}
	
	*//**
	 * slave调用这个方法来通知master自己已经拖完了全量索引数据了
	 *//*
	public void pullIndexFinished(String msg) throws TerminatorIndexWriteException{
		if(this.slaveInvokeTimeoutThread == null){
			logger.warn("超时线程没有开启，说明master没有通知过slave来拖全量索引，拒绝本次请求");
			return;
		}
		
		logger.warn("Slave向Master报告同步全量索引的情况 ==> " + msg);
		
		//当slave拖完数据之后要调用TerminatorIndexWriter的pullIndexFinished方法。
		//没调用一次会将计数器减一，当计数器为0之后，Master才会触发SolrCore的交换。
		if(this.slavePullCount.decrementAndGet() == 0){
			logger.warn("slave已经都拖完了全量索引，开始交换solrCore");
			//让timeout线程正常退出
			this.slaveInvokeTimeoutThread.stopRunning();
			this.slaveInvokeTimeoutThread.interrupt();
			this.slaveInvokeTimeoutThread = null;
			if(this.indexingCore != null){
				synchronized(this.solrCore){			
					this.solrCore = IndexUtils.swapCores(this.solrCore, this.indexingCore);
					try {
						this.indexScheduler.getContext().remove("solrCore");
						this.indexScheduler.getContext().put("solrCore", this.solrCore);
					} catch (SchedulerException e) {
						logger.error("获取SchedulerContext失败，core的交换成功，但是SchedulerContext中的老的SolrCore", e);
					}
				}
				this.indexingCore = null;
				
				//修改全量索引状态,直到这里，整个全量索引过程才真正结束
				IndexContext.isFullIndexing.set(false);
			}
		} else{
			//让slave的timeout监听线程重新开始等待
			this.slaveInvokeTimeoutThread.sleepAgain();
			this.slaveInvokeTimeoutThread.interrupt();
		}
	}
	
	*//**
	 * 返回Date时间点以后的所有文件的文件名列表
	 *//*
	public FetchFileListResponse fetchIncrFileList(final String date){
		logger.warn("Slave取增量xml文件 ,其实时间为 ==> " + date);
		Collection<File> fileList = new LinkedList<File>();
		try {
			fileList = IndexFileUtils.listIncrFile(this.incrXmlSourceDir, date, 100);
		} catch (ParseException e) {
			logger.error("输入的参数格式错误，date:" + date, e);
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

	public void jobExecutionVetoed(JobExecutionContext arg0) {}

	public void jobToBeExecuted(JobExecutionContext arg0) {}

	public void jobWasExecuted(JobExecutionContext jobContext, JobExecutionException jobException) {
		logger.warn("有任务完成，Job full name:" + jobContext.getJobDetail().getFullName());
		if(jobContext.getJobDetail().getFullName().equals(this.solrCore.getName() + INDEX_JOB_GROUP + "." + this.solrCore.getName() + FULL_INDEX_JOB_NAME)){
			logger.warn("全量索引dump任务结束，开始通知slave。");
			//在这个方法中通知slave来拖数据,每次通知一个要进行计数。
			//当slave拖完数据之后要调用TerminatorIndexWriter的pullIndexFinished方法。
			//没调用一次会将计数器减一，当计数器为0之后，Master才会触发SolrCore的交换。
			this.indexingCore = (SolrCore)jobContext.get("newSolrCore");
			this.registFullIndexFileProvider();
			
			//确保Server的线程是可用的
			if(!fileGetServer.isAlive()){
				logger.warn("FileGetServer失效,重新启动该Server.");
				try {
					fileGetServer.start();
				} catch (IOException e) {
					logger.error("启动FileGetServer失败",e);
				}
			}
			
			this.slaveInvokeTimeoutThread = new TimeoutThread(SLAVE_TIMEOUT_THRESHOLD, this, "handlerSlavePullTimeout");
			
			if(slaveServiceList != null && !slaveServiceList.isEmpty()){
				String indexDirStr = indexingCore.getIndexDir();
				File indexDir = new File(indexDirStr);
				File[] files = indexDir.listFiles();
				List<String> fileNameList = new ArrayList<String>(files.length);
				for(File file : files){
					fileNameList.add(file.getName());
				}
				
				FetchFileListResponse fileListResponse = new FetchFileListResponse(TerminatorCommonUtils.getLocalHostIP(),fileGetServerPort,fileNameList);
				
				
				for(SlaveService slaveService : slaveServiceList){
					try {
						slaveService.notify2FetchFullIndexFiles(fileListResponse, IndexFileUtils.getIncreStartDate(this.incrXmlSourceDir));
						this.slavePullCount.incrementAndGet();
					} catch (IOException e) {
						logger.error("读取下次增量开始时间出错", e);
						slaveService.notify2FetchFullIndexFiles(fileListResponse, null);
					}
				}
				//打开slave的超时监听线程，超时时间设为10分钟
				this.slaveInvokeTimeoutThread.start();
			}else{
				this.slaveInvokeTimeoutThread.start();
				try {
					this.slavePullCount.set(1);
					this.pullIndexFinished("<没有slave>");
				} catch (TerminatorIndexWriteException e) {
					logger.error("在没有slave的情况下，直接调用pullIndexFinished出错", e);
				}
			}
		}
	}
	
	*//**
	 * 被TimeoutTread回调的方法，超时发生时被调用
	 *//*
	@SuppressWarnings("unused")
	private void handlerClientFullDumpTimeout(){
		logger.warn("客户端进行全量xml传送的时候发生超时。");
		this.startFullDumpCount.set(1);
		this.finishFullDump();
	}
	
	*//**
	 * 被TimeoutTread回调的方法，超时发生时被调用
	 * @throws TerminatorIndexWriteException 
	 *//*
	@SuppressWarnings("unused")
	private void handlerSlavePullTimeout() throws TerminatorIndexWriteException{
		logger.warn("slave拖全量索引的时候发生超时。");
		this.slavePullCount.set(1);
		this.pullIndexFinished("<slave拖全量索引的时候发生超时>");
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
	
	
}
*/