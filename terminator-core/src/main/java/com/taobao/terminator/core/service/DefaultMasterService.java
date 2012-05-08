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

		//��ʼ���Ƿ���Master����Ϣ
		this.initIsMaster();
		if(!isMaster){
			logger.warn("--"+ solrCore.getName() + " ����������Master��ɫ,�ʲ���Ҫ��ʼ��Master�ķ����༴��������Ӧ��Master��HSF����Ҳ��������Ӧ��ȫ���ļ����߳�.");
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
			logger.error("--"+ solrCore.getName() + " ����Schedulerʧ�ܣ���ʼ��IndexWriterServiceʧ�ܣ�", e);
			throw new RuntimeException("--"+ solrCore.getName() + " ��ʼ��IndexWriterServiceʧ�ܣ�");
		} catch (ParseException e) {
			logger.error("--"+ solrCore.getName() + " ������������ʱģʽ�ַ��� ʧ�ܣ���ʼ��IndexWriterServiceʧ�ܣ�", e);
			throw new RuntimeException("--"+ solrCore.getName() + " ��ʼ��IndexWriterServiceʧ�ܣ�");
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
	/**
	 * ע�������ļ���FileProvider,���ڴ��ļ�·��������ȫ���Ľ��н��б仯����ÿ��ȫ����������֮�����Ҫ���ô˷���<br>
	 * PS.ȫ�����������  Slave��ʼ���ļ�֮ǰ ���ô˷���
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
				logger.warn("[Full-Index]--" + solrCore.getName() + " ȫ���������񼴽���ʼִ�У������״̬��֪���������Slave�������Ա���������Ӧ����.");
				for(SlaveService slaveService : slaveServiceList){
					slaveService.notifySlaveAfterStartFull();
				}
			}
			
			try {
				fullDumpStartTime = IndexFileUtils.getIncreStartDate(incrXmlSourceDir);
			} catch (IOException e) {
				logger.warn("[Full-Index]--"+ solrCore.getName() + " ��ȡ������ʼʱ��ʧ��,�Ե�ǰʱ�����.");
				fullDumpStartTime = IndexFileUtils.formatDate(new Date());
			}
			logger.warn("[Full-Index]--"+ solrCore.getName() + " ȫ����ʼ��ʱ��Ϊ ==> " + fullDumpStartTime + " ȫ����������ô��¼����������ʼ�ļ��е�ʱ�䣬���Զ���ȫ��ʱ�����������.");
		}

		@Override
		public void jobWasExecuted(JobExecutionContext jobContext,JobExecutionException exception) {
			boolean slaveNeedFetchIndex = exception == null;
			boolean fullIndexisOk       = slaveNeedFetchIndex;
			
			logger.warn("[Full-Index] --"+ solrCore.getName() + " ȫ������dump�������  ==> " +(fullIndexisOk ? "[����]":"[�쳣]") + "����.");
			
			indexingCore = (SolrCore)jobContext.get("newSolrCore");
			registFullIndexFileProvider();
			
			if(!fileGetServer.isAlive()){
				logger.warn("--"+ solrCore.getName() + " FileGetServerʧЧ,����������Server.");
				try {
					fileGetServer.start();
				} catch (IOException e) {
					logger.error("����FileGetServerʧ��",e);
				}
			}
			
			if(slaveServiceList != null && !slaveServiceList.isEmpty()){ //��Slave����
				logger.warn("--"+ solrCore.getName() + " ��Slave��������ʼһһ֪ͨSlave��������ȫ���õ�����.");
				if(slaveNeedFetchIndex){ //Master��������
					String indexDirStr = indexingCore.getIndexDir();
					File indexDir = new File(indexDirStr);
					File[] files = indexDir.listFiles();
					List<String> fileNameList = new ArrayList<String>(files.length);
					
					if(files == null || files.length == 0){
						logger.error(" --"+ solrCore.getName() + " ********* ����Ŀ¼  " + indexDirStr + "��û�������ļ�,�˴�ȫ��������!");
						return;
					}
					
					for(File file : files){
						fileNameList.add(file.getName());
					}
					
					FetchFileListResponse fileListResponse = new FetchFileListResponse(TerminatorCommonUtils.getLocalHostIP(),fileGetServerPort,fileNameList);
					
					{
						//������ʱ�����߳�
						logger.warn("--"+ solrCore.getName() + "����NotifySlaveTimeoutThread��ʱ�����߳�.");
						notifySlaveTimeoutThread = new NotifySlaveTimeoutThread(30 * 60 * 1000); //30����
						notifySlaveTimeoutThread.setName("Notify-Slave-TimeOut-Listen-Thread");
						notifySlaveTimeoutThread.start();
					}
					for(SlaveService slaveService : slaveServiceList){
						try{
							slaveService.notifySlaveAfterFull(true, fileListResponse, fullDumpStartTime);
						}catch(Exception e){
							logger.error("ȫ��������֪ͨSlaveʧ��(SlaveSerivce.notifySlaveAfterFull)",e);
						}
						masterStatus.slaveCount.incrementAndGet();
					}
				}else{//Masterȫ�������⣬��֪Slave����Ҫͬ�������ļ���Slaveֻ��Ҫ������Ӧ�ı�־λ
					logger.warn("--"+ solrCore.getName() + "Master��ȫ���쳣�����������ج��һһ��֪Slave��.");
					for(SlaveService slaveService : slaveServiceList){
						try{
							slaveService.notifySlaveAfterFull(false, null, null);
						}catch(Exception e){
							logger.error("--"+ solrCore.getName() + " ȫ��������֪ͨSlaveʧ��(SlaveSerivce.notifySlaveAfterFull)",e);
						}
					}
					
					logger.warn("--"+ solrCore.getName() + " ����ȫ�������Ĺ���ȫ�������ˣ�״̬Ϊȫ����λ��");
					//��д������ʼʱ���ļ�
					IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
					masterStatus.reset();
				}
			}else{//û��Slave����
				masterStatus.slaveCount.set(0);
				if(fullIndexisOk){
					try{
						logger.warn("--"+ solrCore.getName() + " �������û��Slave��ɫ�Ļ���,��ֱ�ӽ���Core.");
						swapCore();
						masterStatus.indexing.set(false);
					}catch(Exception e){
						logger.error("--"+ solrCore.getName() + " ȫ����Ϻ󽻻�Coreʧ��,������Ȼʹ��ԭ����Core�����ṩ����.",e);
					}finally{
						//��д������ʼʱ���ļ�
						IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
					}
				}else{
					logger.warn("--"+ solrCore.getName() + " �˴�ȫ��Dump�����⣬�ʲ�����Core.");
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
			logger.warn("--"+ solrCore.getName() + " һ���������ѿ�ʼ��");
			context.put("isFullIndexing", new Boolean(masterStatus.indexing.get()));
		}

		@Override
		public void jobWasExecuted(JobExecutionContext context,JobExecutionException exception) {
			logger.warn("--"+ solrCore.getName() + " һ�������������������");
			if(exception != null){
				logger.warn("--"+ solrCore.getName() + " �˴�������ʧ�ܸ��� ===>",exception);
			}
		}
	}
	
	private void initIsMaster(){
		String coreName = this.solrCore.getName();
		CoreProperties coreProperties = null;
		try {
			coreProperties = new CoreProperties(solrCore.getResourceLoader().openConfig("core.properties"));
		} catch (IOException e) {
			logger.warn("--"+ solrCore.getName() + " ��ȡ [" + coreName +"] ��core.properties�ļ�ʧ��",e);
			return;
		}
		
		this.isMaster = coreProperties != null && coreProperties.isWriter();
	}

	/**
	 * Master��Ҫ����Slave�����ķ���
	 */
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
		this.fullIndexConsumer = new FullIndexConsumer(this.fullXmlSourceDir, 5);
		this.incrIndexConsumer = new IncrIndexConsumer(this.incrXmlSourceDir, 5);
	}
	/**
	 * ��ʼ�����xml���ݵ�Ŀ¼��������solr homeĿ¼���棬����core�л���ʱ����Ҫ���ġ�
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
		logger.warn("--"+ solrCore.getName() + "[Trigger-Full-Index-Job] ����ȫ��Index����.....");
		try {
			IndexUtils.cleanDir(this.fullXmlSourceDir);
		} catch (IOException e) {
			logger.warn("--"+ solrCore.getName() + "����ȫ��xmlĿ¼ʧ��,path ==> " + this.fullXmlSourceDir.getName(), e);
		} 
		
		try {
			this.indexScheduler.triggerJob(this.solrCore.getName() + FULL_INDEX_JOB_NAME, this.solrCore.getName() + INDEX_JOB_GROUP);
		} catch (SchedulerException e) {
			logger.error("--"+ solrCore.getName() + "��ʼ������Schedulerʧ�ܡ���", e);
			throw new TerminatorMasterServiceException("--"+ solrCore.getName() + "��ʼ������Schedulerʧ�ܡ���");
		}
	}
	
	private MasterStatus masterStatus = new MasterStatus();
	private TimeoutThread fullDumpTimeoutThread = null;
	
	public boolean startFullDump(String clientIp) throws TerminatorMasterServiceException{
		logger.warn("["+ solrCore.getName() + "] ����startFullDump��������ǰMaster��״̬Ϊ ==> " + masterStatus.toString());
		if(masterStatus.remoteClients.contains(clientIp)){
			logger.error("[" + solrCore.getName() +"] IPΪ " +clientIp + " �Ŀͻ����Ѿ����ù�startFullDump������,�ʴ˴ε�����Ч.");
			return false;
		}else{
			masterStatus.remoteClients.add(clientIp);
			logger.warn("[" + solrCore.getName() + "] �Ѿ�����startFull�����Ŀͻ����� ==> " + masterStatus.remoteClients);
		}
		
		if(masterStatus.remoteClienCount.incrementAndGet() == 1){
			{
				logger.warn("--"+ solrCore.getName() + " ����FullDumpTimeoutThread��ʱ�����߳�,��ʱʱ��Ϊ :" + 12 * 60 * 60 * 1000 + "ms,����ڸ�ʱ��������û�д�����ϣ�����������Ͷ��������ݽ�������.");
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
		logger.warn("--"+ solrCore.getName() + " ����fullDump����,��ǰMaster��״̬Ϊ ==> " + masterStatus.toString());
		if((masterStatus.started.get() || masterStatus.dumping.get()) && !masterStatus.finished.get() && masterStatus.remoteClients.contains(clientIp)){
			masterStatus.dumping.set(true);
			fullIndexConsumer.consum(indexData);
			return true;
		}else{
			logger.warn("--"+ solrCore.getName() + " [Dump]�ܾ�����ȫ������,���ø÷����Ŀͻ���IPΪ : + " +	clientIp + "��ǰ״̬Ϊ ==> " + masterStatus.toString());
			return false;
		}
	}
	
	public boolean finishFullDump(String clientIp) {
		logger.warn("--"+ solrCore.getName() + " ����finishFullDump����,��ǰMaster��״̬Ϊ ==> " + masterStatus.toString());
		
		if(masterStatus.remoteClients.contains(clientIp)){
			masterStatus.remoteClients.remove(clientIp);
		}else{
			logger.warn("[" + solrCore.getName() + "] IPΪ  " + clientIp +" �Ŀͻ��˻���û�е��ù�startFull��������ǰ���Ϸ���ClientIps��:" + masterStatus.remoteClients);
			return false;
		}
		
		if(masterStatus.started.get() && !masterStatus.finished.get() ){
			if(masterStatus.remoteClienCount.decrementAndGet() == 0){
				fullIndexConsumer.finish();	
				fullDumpTimeoutThread.finish();
				masterStatus.finished.set(true);
			} else{	
				logger.warn("["+ solrCore.getName() + "]  �ͻ��˵�����һ��ȫ������xml���ݴ��ͽ������󣬵�ǰ����[" + this.masterStatus.remoteClienCount.get() + "]���ͻ������ڴ������ݡ�");
			}
			return true;
		}else{
			logger.error("["+ solrCore.getName() + "] �ܾ���������(finishFullDump),��ǰ״̬Ϊ ==> " + masterStatus.toString());
			return false;
		}
	}
	
	/**
	 * ȫ��Dump��ʱ�����߳�
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
			logger.warn("[Time-Out-Thread] ������ʱ�̣߳�TIME_OUT=[" + timeout + " ms]");
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
			logger.warn("[Time-Out-Thread] ������ʱ�߳�.");
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
			logger.warn("����NotifySlaveTimeoutThread�ĳ�ʱ�¼�.");
			masterStatus.slaveCount.set(1);
			try {
				pullIndexFinished("Slaveû����Timeoutʱ���ڸ������.ɶ�������ˣ�Master�ø�ɶ��ɶ.");
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
			logger.warn("[Time-Out-Thread] ȫ��Dump���ݵĳ�ʱ�¼�.");
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
	 * slave�������������֪ͨmaster�Լ��Ѿ�������ȫ������������
	 */
	public void pullIndexFinished(String msg) throws TerminatorMasterServiceException{
		logger.warn("--"+ solrCore.getName() + " Slave��Master����ͬ��ȫ����������� ==> " + msg);
		
		if(this.masterStatus.slaveCount.decrementAndGet() == 0){
			logger.warn("--"+ solrCore.getName() + " slave�Ѿ���������ȫ����������ʼ����SolrCore");
			try{
				this.swapCore();
				this.solrCore.getCoreDescriptor().getCoreContainer().persist();
				IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, fullDumpStartTime);
				notifySlaveTimeoutThread.finish();
				this.masterStatus.indexing.set(false);
			}catch(Exception e){
				logger.error("--"+ solrCore.getName() + "ȫ����Ϻ󽻻�Coreʧ��.",e);
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
					logger.error("--"+ solrCore.getName() + "��ȡSchedulerContextʧ�ܣ�core�Ľ����ɹ�������SchedulerContext�е��ϵ�SolrCore", e);
				}
			}
			this.indexingCore = null;
		}
	}
	
	/**
	 * ����Dateʱ����Ժ�������ļ����ļ����б�
	 */
	public FetchFileListResponse fetchIncrFileList(final String date){
		logger.warn("--"+ solrCore.getName() + " Slaveȡ����xml�ļ� ,��ʼʱ��Ϊ ==> " + date);
		Collection<File> fileList = new LinkedList<File>();
		try {
			fileList = IndexFileUtils.listIncrFile(this.incrXmlSourceDir, date, 100);
		} catch (ParseException e) {
			logger.error("--"+ solrCore.getName() + " ����Ĳ�����ʽ����date:" + date, e);
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
