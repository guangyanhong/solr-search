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
	private static final String INCR_INDEX_JOB_CYCLE = "0 0/2 * * * ?"; //ÿ�����ӳ���һ����������
	private static final String INCR_XML_REMOVE_JOB_CYCLE = "0 0 0 * * ?"; //ÿ��0ʱ����һ������xml��������

	private static final int    INSTANT_INDEX_JOB_NUM       = 0x0005;

	
	private static Log logger = LogFactory.getLog(OldMasterService.class);
	
	private IndexConsumer fullIndexConsumer;
	private IndexConsumer incrIndexConsumer;
	
	private ThreadPoolExecutor instantIndexJobs;
	
	*//**
	 * ��ǰ����ʹ��core
	 *//*
	private SolrCore solrCore;
	
	*//**
	 * �ڽ���ȫ������ʱ�����������õ�xml���ݱ�д����ļ��У�data import������ļ����ж�ȡxml�ļ�����������
	 *//*
	private File fullXmlSourceDir;
	
	*//**
	 * �ڽ�����������ʱ�����������õ�xml���ݱ�д����ļ��У�data import������ļ����ж�ȡxml�ļ�����������
	 *//*
	private File incrXmlSourceDir;
	
	*//**
	 * ��¼�����ü���startFullDump����
	 *//*
	private AtomicInteger startFullDumpCount;
	
	private IndexFileSearcher fullIndexFileSearcher;
	
	private IndexFileSearcher incrIndexFileSearcher;
	
	private Scheduler indexScheduler;
	
	private MastersGroupConfig groupConfig = null;
	
	private boolean isMaster = false;
	
	private Set<String> slaveIps = null;

	*//**
	 * ������¼���������ݵ�salve�ĸ���
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
	 * ���ͻ��˷���һ��ȫ�������ʱ�򣬻Ὺ��һ����ʱ�߳�
	 * ÿ���ͻ��˵�����һ��fullDump�������ͻ��ǰ�����
	 * ��ʱ�߳��жϣ�Ȼ�����¿�һ����ʱ�̡߳�����ͻ�����
	 * ������һ��fullDump֮���ڳ�ʱʱ����û���ٵ���fullDump
	 * ����fullDumpFinish֮һ�ķ�������ô�ͻᴥ����ʱ���������
	 *//*
	private TimeoutThread clientInvokeTimeoutThread;
	
	*//**
	 * ���slave�ڳ�ʱʱ��֮��û�е���pullIndexFinished����
	 * ��ô�ͻᴥ����ʱ���������ÿ��slave����pullIndexFinished
	 * ���� �ͻ��жϵ�ǰ�ĳ�ʱ�̣߳��������¿���һ����
	 *//*
	private TimeoutThread slaveInvokeTimeoutThread;
	
	*//**
	 * ����ȫ������������solrCore�л�֮ǰ��������������
	 * ����ȫ���������Ǹ�solrCore�����core��FullIndexWriteJob������
	 * ��FullIndexWriteJob������ʱ��ͨ��JobListener�ӿڴ�������
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

		//��ʼ���Ƿ���Master����Ϣ
		this.initIsMaster();
		if(!isMaster){
			logger.warn("����������Master��ɫ,�ʲ���Ҫ��ʼ��Master�ķ����༴��������Ӧ��Master��HSF����Ҳ��������Ӧ��ȫ���ļ����߳�.");
			return;
		}

		//����hsf����
		this.publishHsf();
		
		//����hsf���� ==> �ڲ�������Դ���ݵĴ������
		this.subscribeHsf();
		
		//��������IndexConsumer
		this.initIndexConsumer();
		
		//��ʼ������file searcher
		this.initFileSearcher();
		
		// ��ʼ��������ȫ������
		try {
			this.initScheduler();
		} catch (SchedulerException e) {
			logger.error("����Schedulerʧ�ܣ���ʼ��IndexWriterServiceʧ�ܣ�", e);
			throw new RuntimeException("��ʼ��IndexWriterServiceʧ�ܣ�");
		} catch (ParseException e) {
			logger.error("������������ʱģʽ�ַ��� ʧ�ܣ���ʼ��IndexWriterServiceʧ�ܣ�", e);
			throw new RuntimeException("��ʼ��IndexWriterServiceʧ�ܣ�");
		}		
		
		//�����ļ����Ƽ�������
		this.startFileGetServer();
	}
	
	private void startFileGetServer() {
		String portStr = (String)args.get("fileGetPort");
		if(portStr != null){
			fileGetServerPort = Integer.valueOf(portStr);
		}
		
		logger.warn("����FileGetServer,host ==> 127.0.0.1  port ==> " + fileGetServerPort);
		fileGetServer = new FileGetServer(TerminatorCommonUtils.getLocalHostIP(), fileGetServerPort);
		
		//�������ļ��в��ᷢ���仯������������Ŀ¼�ļ���ʱ������ȫ���Ľ��ж������仯�ģ�����Ҫ��̬��ע��
		IncrXmlFileProvider increFileProvider = new IncrXmlFileProvider(incrXmlSourceDir);
		fileGetServer.register(IncrXmlFileProvider.type, increFileProvider);
		
		try {
			if(!fileGetServer.isAlive()){
				fileGetServer.start();
				this.fileGetServerPort = fileGetServer.getPort();
				logger.warn("FileGetServer�Ķ˿�Ϊ ==> " + this.fileGetServerPort);
			}
		} catch (IOException e) {
			logger.error("����FileGetServerʧ��",e);
			throw new RuntimeException("����FilGetServerʧ��",e);
		}
	}
	*//**
	 * ע�������ļ���FileProvider,���ڴ��ļ�·��������ȫ���Ľ��н��б仯����ÿ��ȫ����������֮�����Ҫ���ô˷���<br>
	 * PS.ȫ�����������  Slave��ʼ���ļ�֮ǰ ���ô˷���
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
		
		//ȫ����������ÿ�ν��յ�ȫ������ʱ��MasterService��������
		JobDetail fullIndexJobDetail = new JobDetail(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, FullIndexWriteJob.class);
		fullIndexJobDetail.setDurability(true);
		//�����������񣬶�ʱ����
		JobDetail incrIndexJobDetail = new JobDetail(this.solrCore.getName() + INCR_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP, IncrIndexWriteJob.class);
		
		//����xml��������ÿ�����һ��
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
			logger.warn("��ȡ [" + coreName +"] ��core.properties�ļ�ʧ��",e);
			return;
		}
		
		this.isMaster = coreProperties != null && coreProperties.isWriter();
	}

	*//**
	 * Master��Ҫ����Slave�����ķ���
	 *//*
	private void  subscribeHsf(){
		String coreName = solrCore.getName();
		logger.warn("��������Ӧ��  " + coreName + "  ��Master��ɫ,����Ҫ�������Ӧ��Slave�������ڲ�ͨѶ����.");
		String[] ss = TerminatorCommonUtils.splitCoreName(coreName);
		if(ss == null){
			logger.error("CoreName ==> " + coreName + " �ֽ��serviceName groupNameʧ��.");
		}
		
		logger.warn("��ZooKeeper�ϻ�ȡ�� " + coreName + "��Ӧ��GroupConfig��Ϣ.");
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
		logger.warn("SolrCore ==> " + coreName + "��Ӧ��Slave���������˱仯�������������׶Σ�(����)����Slave�������ڲ�ͨѶ����.");
		
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
			logger.warn("����  HsfVerion ==> " + hsfVersion);
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
		logger.warn("������Ӧ��  " + coreName +"  ��HSF����.");
		String hsfVersion = TerminatorHSFContainer.Utils.genearteVersion(ServiceType.writer, coreName);
		logger.warn("��������Ӧ��  " + coreName + "  ��Master��ɫ����������ѿͻ��˶��Ⱪ¶д��������.");
		logger.warn("����  HsfVerison ==> " + hsfVersion);
		try {
			TerminatorHSFContainer.publishService(this, MasterService.class.getName(), hsfVersion,10 * 1000); //��ʱʱ�����ó�һЩ 
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
	 * ��ʼ�����xml���ݵ�Ŀ¼��������solr homeĿ¼���棬����core�л���ʱ����Ҫ���ġ�
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
			logger.warn("�ͻ���ȫ��������ʱ�߳�Ϊnull��˵��û�е��ù�startFulLDump�������ܾ���������");
			return;
		}
		//ȫ��������-1��������0��ʱ��ʼ��������
		if(this.startFullDumpCount.decrementAndGet() == 0){
			logger.warn("���еĿͻ��˶��Ѿ�֪ͨ���ݴ�������ˣ�����IndexConsumer��");
			this.fullIndexConsumer.finish();		
			//�����ڳ�ʱʱ��֮�������������ÿͻ��˳�ʱ�����߳���������
			this.clientInvokeTimeoutThread.stopRunning();
			this.clientInvokeTimeoutThread.interrupt();
			this.clientInvokeTimeoutThread = null;
		} else{	
			//�ÿͻ��˳�ʱ�����߳����¿�ʼһ�μ�������
			this.clientInvokeTimeoutThread.sleepAgain();
			this.clientInvokeTimeoutThread.interrupt();
			logger.warn("�ͻ��˵�����һ��ȫ������xml���ݴ��ͽ�������" + "���ǻ��������Ŀͻ���û�е��ã���ǰ����<" + this.startFullDumpCount.get() + ">���ͻ������ڴ������ݡ�");
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
			logger.warn("��������...");
		}
	}
	
	public void incrDump(byte[] indexData) throws TerminatorIndexWriteException{		
		this.incrIndexConsumer.consum(indexData,null);
	}
	
	public void instantDump(byte[] indexData) throws TerminatorIndexWriteException{
		if(IndexContext.isFullIndexing.get() || IndexContext.isIncrIndexing.get()){
			logger.warn("��ǰ��ȫ������������������ִ�У��ܾ�����ʵʱ����д������");
			return;
		} else{
			InstantSolrXMLDataImporter indexImporter =  new InstantSolrXMLDataImporter(this.solrCore, indexData);
			this.instantIndexJobs.execute(indexImporter);
		}
	}

	public boolean startFullDump() throws TerminatorIndexWriteException{
		this.startFullDumpCount.incrementAndGet();
		if(this.startFullDumpCount.get() == 1 && IndexContext.isFullIndexing.get()){
			logger.warn("��һ��������û����ɣ��ܾ�������������");
			//��startFullDumpCount��1
			this.startFullDumpCount.decrementAndGet();
			return false;
		}
		
		if(this.startFullDumpCount.get() == 1 && !IndexContext.isFullIndexing.get()){
			logger.warn("�ͻ��˵�����һ��ȫ�������������ǵ�һ�Σ�ȫ��������ʼ ---- ����ȫ������.");
			IndexContext.isFullIndexing.set(true);
			try {
				IndexUtils.cleanDir(this.fullXmlSourceDir);
			} catch (IOException e) {
				logger.warn("����ȫ��xmlĿ¼ʧ�ܡ���dir:" + this.fullXmlSourceDir.getName(), e);
			} 
			
			try {
				this.fullIndexConsumer.start();
				this.indexScheduler.triggerJob(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
			} catch (SchedulerException e) {
				logger.error("��ʼ������Schedulerʧ�ܡ���", e);
				throw new TerminatorIndexWriteException("��ʼ������Schedulerʧ�ܡ���");
			}
			
			this.clientInvokeTimeoutThread = new TimeoutThread(CLIENT_TIMEOUT_THRESHOLD, this, "handlerClientFullDumpTimeout");
			this.clientInvokeTimeoutThread.start();
		} else{
			logger.warn("�ͻ��˵�����һ��ȫ���������󣬵���֮ǰ�Ѿ��пͻ��˵��ù��������ǵ�<"  + this.startFullDumpCount.get() + ">������");
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
	 * slave�������������֪ͨmaster�Լ��Ѿ�������ȫ������������
	 *//*
	public void pullIndexFinished(String msg) throws TerminatorIndexWriteException{
		if(this.slaveInvokeTimeoutThread == null){
			logger.warn("��ʱ�߳�û�п�����˵��masterû��֪ͨ��slave����ȫ���������ܾ���������");
			return;
		}
		
		logger.warn("Slave��Master����ͬ��ȫ����������� ==> " + msg);
		
		//��slave��������֮��Ҫ����TerminatorIndexWriter��pullIndexFinished������
		//û����һ�λὫ��������һ����������Ϊ0֮��Master�Żᴥ��SolrCore�Ľ�����
		if(this.slavePullCount.decrementAndGet() == 0){
			logger.warn("slave�Ѿ���������ȫ����������ʼ����solrCore");
			//��timeout�߳������˳�
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
						logger.error("��ȡSchedulerContextʧ�ܣ�core�Ľ����ɹ�������SchedulerContext�е��ϵ�SolrCore", e);
					}
				}
				this.indexingCore = null;
				
				//�޸�ȫ������״̬,ֱ���������ȫ���������̲���������
				IndexContext.isFullIndexing.set(false);
			}
		} else{
			//��slave��timeout�����߳����¿�ʼ�ȴ�
			this.slaveInvokeTimeoutThread.sleepAgain();
			this.slaveInvokeTimeoutThread.interrupt();
		}
	}
	
	*//**
	 * ����Dateʱ����Ժ�������ļ����ļ����б�
	 *//*
	public FetchFileListResponse fetchIncrFileList(final String date){
		logger.warn("Slaveȡ����xml�ļ� ,��ʵʱ��Ϊ ==> " + date);
		Collection<File> fileList = new LinkedList<File>();
		try {
			fileList = IndexFileUtils.listIncrFile(this.incrXmlSourceDir, date, 100);
		} catch (ParseException e) {
			logger.error("����Ĳ�����ʽ����date:" + date, e);
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
		logger.warn("��������ɣ�Job full name:" + jobContext.getJobDetail().getFullName());
		if(jobContext.getJobDetail().getFullName().equals(this.solrCore.getName() + INDEX_JOB_GROUP + "." + this.solrCore.getName() + FULL_INDEX_JOB_NAME)){
			logger.warn("ȫ������dump�����������ʼ֪ͨslave��");
			//�����������֪ͨslave��������,ÿ��֪ͨһ��Ҫ���м�����
			//��slave��������֮��Ҫ����TerminatorIndexWriter��pullIndexFinished������
			//û����һ�λὫ��������һ����������Ϊ0֮��Master�Żᴥ��SolrCore�Ľ�����
			this.indexingCore = (SolrCore)jobContext.get("newSolrCore");
			this.registFullIndexFileProvider();
			
			//ȷ��Server���߳��ǿ��õ�
			if(!fileGetServer.isAlive()){
				logger.warn("FileGetServerʧЧ,����������Server.");
				try {
					fileGetServer.start();
				} catch (IOException e) {
					logger.error("����FileGetServerʧ��",e);
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
						logger.error("��ȡ�´�������ʼʱ�����", e);
						slaveService.notify2FetchFullIndexFiles(fileListResponse, null);
					}
				}
				//��slave�ĳ�ʱ�����̣߳���ʱʱ����Ϊ10����
				this.slaveInvokeTimeoutThread.start();
			}else{
				this.slaveInvokeTimeoutThread.start();
				try {
					this.slavePullCount.set(1);
					this.pullIndexFinished("<û��slave>");
				} catch (TerminatorIndexWriteException e) {
					logger.error("��û��slave������£�ֱ�ӵ���pullIndexFinished����", e);
				}
			}
		}
	}
	
	*//**
	 * ��TimeoutTread�ص��ķ�������ʱ����ʱ������
	 *//*
	@SuppressWarnings("unused")
	private void handlerClientFullDumpTimeout(){
		logger.warn("�ͻ��˽���ȫ��xml���͵�ʱ������ʱ��");
		this.startFullDumpCount.set(1);
		this.finishFullDump();
	}
	
	*//**
	 * ��TimeoutTread�ص��ķ�������ʱ����ʱ������
	 * @throws TerminatorIndexWriteException 
	 *//*
	@SuppressWarnings("unused")
	private void handlerSlavePullTimeout() throws TerminatorIndexWriteException{
		logger.warn("slave��ȫ��������ʱ������ʱ��");
		this.slavePullCount.set(1);
		this.pullIndexFinished("<slave��ȫ��������ʱ������ʱ>");
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