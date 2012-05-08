package com.taobao.terminator.core.service.inner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
import org.quartz.SimpleTrigger;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.springframework.scheduling.SchedulingException;

import com.taobao.terminator.common.CoreProperties;
import com.taobao.terminator.common.ServiceType;
import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.TerminatorHsfPubException;
import com.taobao.terminator.common.TerminatorHsfSubException;
import com.taobao.terminator.common.TerminatorMasterServiceException;
import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.common.protocol.FetchFileListResponse;
import com.taobao.terminator.common.protocol.MasterService;
import com.taobao.terminator.common.stream.FileGetClient;
import com.taobao.terminator.common.stream.FileGetResponse;
import com.taobao.terminator.core.index.FecthIncrXmlFilesJob;
import com.taobao.terminator.core.index.IncrIndexFileSearcher;
import com.taobao.terminator.core.index.IncrIndexWriteJob;
import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.index.OldIncrXmlRemoveJob;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.index.stream.IncrXmlFileProvider;
import com.taobao.terminator.core.service.Lifecycle;
import com.taobao.terminator.core.util.IndexFileUtils;
import com.taobao.terminator.core.util.IndexUtils;

/**
 * 用于Slave同步Master数据的Service不对外暴露该服务
 * 
 * @author yusen
 */
public class DefaultSlaveService implements SlaveService,NamedListInitializedPlugin,Lifecycle{
	
	private static Log logger = LogFactory.getLog(DefaultSlaveService.class);
	
	private SolrCore solrCore;

	private File incrXmlSourceDir;
	
	private IndexFileSearcher incrIndexFileSearcher;
	
	private Scheduler indexScheduler;
	
	private MasterService masterService ;
	
	private boolean isSlave = true;
	
	private long syncFromMasterPeriod = 5 * 60; //单位：秒
	
	private String incrCronExpression = "0 0/2 * * * ?";
	
	private String incrXmlRemoveCronExpression = "0 0 0 * * ?";
	
	private AtomicBoolean isFullIndexing = new AtomicBoolean(false);
	
	@SuppressWarnings("unchecked")
	private NamedList args = null;
	
	protected SimpleDateFormat df = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
	
	public DefaultSlaveService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.createXmlDataDir();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args){
		this.args = args;
		//初始化是否是Master的信息
		this.initIsSlave();
		if(!isSlave){
			return;
		}
		//初始增量拖文件的频率
		this.initPeriod();

		//发布hsf服务
		this.publishHsf();
		
		//订阅hsf服务 ==> 内部的索引源数据的传输服务
		this.subscribeHsf();
		
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
	}
	
	private void initPeriod() {
		String syncFromMasterPeriodStr = (String)args.get("syncFromMasterPeriod");
		
		try{
			syncFromMasterPeriod = Long.valueOf(syncFromMasterPeriodStr);
		}catch(NumberFormatException e){
			syncFromMasterPeriod = 5 * 60;
		}
		String s = (String)args.get("incrCronExpression");
		if(StringUtils.isNotBlank(s)){
			incrCronExpression = s; 
		}
		
		s = (String)args.get("incrXmlRemoveCronExpression");
		if(StringUtils.isNotBlank(s)){
			this.incrXmlRemoveCronExpression = s;
		}
		logger.warn("Slave同步Master的增量XML文件的频率为 ==> " + syncFromMasterPeriod +  " 秒");
	}

	public void initScheduler()  throws SchedulerException, ParseException{
		
		JobDetail incrIndexWriteJobDetail = new JobDetail(solrCore.getName() + "-IncrIndexJob", solrCore.getName() + "-JobDetail", IncrIndexWriteJob.class);
		CronTrigger incrIndexWriteJobTrigger = new CronTrigger(solrCore.getName() + "-IncrIndexWriteJobTrigger", solrCore.getName() + "-trigger", incrCronExpression);
		
		long startTime = System.currentTimeMillis() + syncFromMasterPeriod * 1000L;
        SimpleTrigger fetchIncrXmlFilesJobTrigger = new SimpleTrigger("fetchIncrXmlFilesJobTrigger", "trigger", new Date(startTime), null, SimpleTrigger.REPEAT_INDEFINITELY, syncFromMasterPeriod * 1000L);
		
		JobDetail fechIncrXmlFilesJobDetail = new JobDetail("FecthIncrXmlFilesJob", "index", FecthIncrXmlFilesJob.class);
		
		//增量xml清理任务，每天触发一次
		JobDetail incrXmlRemoveJob = new JobDetail(solrCore.getName() + "-IncrXmlRemoveJob", solrCore.getName() + "-JobDetail", OldIncrXmlRemoveJob.class);
		CronTrigger xmlRemoveTrigger = new CronTrigger(solrCore.getName() + "-IncrXmlRemoveTrigger", solrCore.getName() + "-trigger", this.incrXmlRemoveCronExpression);
		
		DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
		SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
		RAMJobStore jobStore = new RAMJobStore();
		jobStore.setMisfireThreshold(60000);
		schedulerFactory.createScheduler(this.solrCore.getName() + "-scheduler", this.solrCore.getName() + "-scheulerInstance", threadPool, jobStore);
		threadPool.initialize();
		this.indexScheduler = schedulerFactory.getScheduler(this.solrCore.getName() + "-scheduler");
		
		this.indexScheduler.getContext().put("solrCore", this.solrCore);
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-fetchIncrFilesJob", new FetchIncrFilesJob());
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-logger", logger);
		
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-incrIndexFileSearcher", this.incrIndexFileSearcher);
		this.indexScheduler.getContext().put(this.solrCore.getName() + "-incrXmlRootDir", this.incrXmlSourceDir.getAbsolutePath());
		
		this.indexScheduler.addJobListener(new SlaveIncrIndexJobListener());
		incrIndexWriteJobDetail.addJobListener("SlaveIncrIndexJobListener");
		this.start();
		
		this.indexScheduler.scheduleJob(incrIndexWriteJobDetail, incrIndexWriteJobTrigger);
		this.indexScheduler.scheduleJob(fechIncrXmlFilesJobDetail, fetchIncrXmlFilesJobTrigger);	
		this.indexScheduler.scheduleJob(incrXmlRemoveJob, xmlRemoveTrigger);
	}
	
	
	private class SlaveIncrIndexJobListener implements JobListener{
		@Override
		public String getName() {
			return "SlaveIncrIndexJobListener";
		}

		@Override
		public void jobExecutionVetoed(JobExecutionContext context) {/* nothing to do */}

		@Override
		public void jobToBeExecuted(JobExecutionContext context) {
			logger.warn("--"+ solrCore.getName() + "一次增量消费开始。");
			context.put("isFullIndexing", new Boolean(isFullIndexing.get()));
		}

		@Override
		public void jobWasExecuted(JobExecutionContext context, JobExecutionException exception) {
			logger.warn("一次增量消费任务结束。");
			if(exception != null){
				logger.warn("此次增量以失败告终 ===>",exception);
			}
		}
	}
	
	public class FetchIncrFilesJob{
		public void fetch() {
			long startTime = System.currentTimeMillis();
			logger.warn(" --"+ solrCore.getName() + "[" + solrCore.getName() + "] 获取增量文件,与Master的增量XML文件保持一致.");
			
			String date = null;
			String incrDateRootDir = solrCore.getCoreDescriptor().getCoreContainer().getSolrHome() + File.separator + solrCore.getCoreDescriptor().getInstanceDir() + File.separator + "incr_xml_source";
			try {
				date = IndexFileUtils.getSyncXmlFileDate(incrDateRootDir);
			} catch (IOException e1) {
				logger.error(" [" + solrCore.getName() + "] 获取增量时间失败,时间为‘ ’，Master会返回所有的文件列表",e1);
			}
			if(date == null){
				date = "";
			}
			
			//通过Master的HSF服务，获取需要复制的文件列表
			FetchFileListResponse fileListRes = null;
			try{
				fileListRes = masterService.fetchIncrFileList(date);
			}catch(Exception e){
				logger.error("--"+ solrCore.getName() + " 本机(Slave)从Master获取增量xml文件列表失败,date ==> " + date , e);
				return;
			}
			
			String masterIp = fileListRes.getMasterIp();
			int    port     = fileListRes.getPort();
			List<String> fileNameList = fileListRes.getFileNameList();
			
			
			if(fileNameList == null || fileNameList.isEmpty()){
				logger.warn("[" + solrCore.getName() + "] 增量的xml文件列表为空,返回.");
				return;
			}
			
			logger.warn(" [" + solrCore.getName() + "] 从Mster主机复制增量的xml文件，Maser ==> " + masterIp +":" + port + " 需要复制的文件个数  ==> " + fileNameList.size());
			FileGetClient fileGetClient = new FileGetClient(masterIp, port);
			
			long endTime = 0L;
			
			Collections.sort((List<String>)fileNameList, new Comparator<String>(){
				public int compare(String arg0, String arg1) {
					return arg0.compareTo(arg1);
				}
			});
			
			List<String> pendingFileList = new LinkedList<String>();
			for(String name : fileNameList){
				logger.warn(" [" + solrCore.getName() + "] 获取增量的xml文件 ==> " + name);
			
				Date d = null;
				try {
					d = df.parse(name);
				} catch (ParseException e1) {
					logger.error(" [" + solrCore.getName() + "] 增量文件文件名格式不正确  ==> " + name);
					continue;
				}
				
				if(d.getTime() > endTime){
					endTime =  d.getTime();
				}
				
				String filePath = null;
				try {
					filePath = IndexFileUtils.generatePathFromFileName(incrXmlSourceDir, name, IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue());
				} catch (ParseException e) {
					logger.error(" [" + solrCore.getName() + "] 文件名 ==> " + name + " 不是合法的文件名，必须是  " + IndexEnum.DATE_TIME_PATTERN.getValue() + " 格式的文件名   继续.",e);
					continue;
				}
				
				File tempFile = new File(filePath);
				FileOutputStream fileOutputStream = null;
				try {
					fileOutputStream = new FileOutputStream(tempFile);
				} catch (FileNotFoundException e1) {
					logger.error(e1,e1);
				}
				
				//从Master上获取增量XML文件
				try {
					fileGetClient.doGetFile(IncrXmlFileProvider.type, name, fileOutputStream);
				} catch (IOException e1) {
					logger.error("从Master获取增量的XML文件失败,忽略此文件，继续复制文件.fileName ==> " + name,e1);
					pendingFileList.add(name);
					continue;
				}finally{
					if(fileOutputStream != null)
						try {
							fileOutputStream.close();
						} catch (IOException e) {
							logger.error(e,e);
						}
				}
				
				String tempFileName = tempFile.getName();
				String finalName = tempFileName.replaceAll(IndexEnum.TMP_INDEX_FILE_SUFFIX.getValue(),IndexEnum.INDEX_FILE_SUFFIX.getValue());
				if(!tempFile.renameTo(new File(tempFile.getParent(),finalName))){
					logger.error("将文件" + tempFile.getAbsolutePath() +  "重命名为" + "" + "失败！");
					pendingFileList.add(name);
				} 
			}
			
			if(pendingFileList != null && !pendingFileList.isEmpty()){
				logger.error("此次同步失败的文件有:" + pendingFileList.toArray());
			}
			
			//重写本地的下次增量开始的时间
			Date endDate = new Date(endTime);
			String endDateStr = df.format(endDate);
			
			logger.warn(" [" + solrCore.getName() + "] 重写下次增量的开始时间  ==>" + endDateStr);
			IndexFileUtils.writeSyncXmlFileDate(incrDateRootDir, endDateStr);
			
			logger.warn("[" + solrCore.getName() + "] 同步增量xml文件完毕，功耗时间  ==> [" + (System.currentTimeMillis() - startTime)/1000 + "] 毫秒");
		}
	}
	
	/**
	 * Master机器的全量索引构建完毕，通知本机(Slave)从Master上同步最新的索引文件
	 */
	public void notifySlaveAfterFull(boolean slaveNeedFetchIndex,FetchFileListResponse fileListResponse,String incrDate){
		if(slaveNeedFetchIndex){
			logger.warn(" [" + solrCore.getName() + "] Master机器全量成功构建完毕，本机(Slave)开始从Master上下载全量后的索引文件. ==>" + fileListResponse.toString());
			Thread thread = new Thread(new FechFullIndexFilesJob(fileListResponse, incrDate));
			thread.setName("FetchIndexFileFromMaster-Thread");
			thread.start();
		}else{ //Master的全量索引有问题，不需要同步有问题的全量索引文件，故只需要设置标志位
			logger.warn("[" + solrCore.getName() +"] Master传来不幸的消息，Master全量Build失败了，所以本机器(Slave)不做索引文件的同步操作，直接设置isIndexing标志位为false,以关闭对增量索引的阻止.");
			isFullIndexing.set(false);
		}
	}
	
	@Override
	public void notifySlaveAfterStartFull() {
		logger.warn("[" + solrCore.getName() +"] Master机器开始全量索引，故本机(Slave)的增量任务停止进行。");
		this.isFullIndexing.set(true);
	}
	
	protected class FechFullIndexFilesJob implements Runnable{
		private FetchFileListResponse fileListResponse;
		private String incrDate = null;
		
		public FechFullIndexFilesJob(FetchFileListResponse fileListResponse,String incrDate) {
			this.fileListResponse = fileListResponse;
			this.incrDate = incrDate;
		}
		

		@Override
		public void run() {
			try{
				doRun();
			}finally{
				//不管取索引怎么样，是否有异常，都要保证把这个标志位设置回去，免得增量任务全部拒绝掉。
				isFullIndexing.set(false);
			}
		}
		
		public void doRun() {
			String masterIp = fileListResponse.getMasterIp();
			int port        = fileListResponse.getPort();
			FileGetClient fileGetClient = new FileGetClient(masterIp, port);
			List<String> fileNameList = fileListResponse.getFileNameList();
			SolrCore newSolrCore = null;
			
			logger.warn("从Mster主机复制全量的索引文件，Maser ==> " + masterIp +":" + port);
			logger.warn("新建SolrCore,以容纳新的索引文件.");
			try {
				newSolrCore = IndexUtils.newSolrCore(solrCore);
			} catch (Exception e) {
				logger.error("新建SolrCore失败.",e);
				return;
			}
				
			String dataDirStr = newSolrCore.getDataDir();
			File dataDir = new File(dataDirStr);
			File indexDir = new File(dataDir,"index");
			
			if(indexDir.exists()){
				logger.warn("清空索引文件目录 ==> " + indexDir.getAbsolutePath());
				try {
					FileUtils.cleanDirectory(indexDir);
				} catch (IOException e1) {
					logger.error("清空索引文件目录失败 ==> " + indexDir.getAbsolutePath(),e1);
				}
			}else{
				indexDir.mkdirs();
			}
			
			List<String> pendingList = new ArrayList<String>();
			
			logger.warn("需要获取的索引文件有 " + fileNameList.size() + "个 ");
			for(String name : fileNameList){
				logger.warn("获取索引文件 ==> " + name);
				File indexFile = new File(indexDir,name);
				FileOutputStream fileOutputStream = null;
				try {
					fileOutputStream = new FileOutputStream(indexFile);
					int code = fileGetClient.doGetFile(FullIndexFileProvider.type, name, fileOutputStream);
					if(FileGetResponse.SUCCESS != code){
						logger.error("获取文件失败，文件名 ==> " + name + " error-code :" + code);
						pendingList.add(name);
						if(fileOutputStream != null){
							try {
								fileOutputStream.close();
							} catch (IOException e) {
								logger.error(e,e);
							}
						}
						continue;
					}
				} catch (Exception e) {
					logger.error("从Master上下载文件失败  name ==> " + name + "  type ==> fullIndexFiles",e);
					pendingList.add(name);
					if(fileOutputStream != null){
						try {
							fileOutputStream.close();
						} catch (IOException e1) {
							logger.error(e,e);
						}
					}
					continue;
				} finally{
					if(fileOutputStream != null){
						try {
							fileOutputStream.close();
						} catch (IOException e) {
							logger.error(e,e);
						}
					}
				}
			}

			String localIp = TerminatorCommonUtils.getLocalHostIP();
			
			if(pendingList.isEmpty()){
				logger.warn("本机(Slave)从Master机器上拉全量索引文件成功，切换新的Core.");
				
				boolean swapSuc = true;
				try{
					synchronized(solrCore){
						solrCore = IndexUtils.swapCores(solrCore, newSolrCore);
						solrCore.getCoreDescriptor().getCoreContainer().persist();
						try {
							indexScheduler.getContext().remove("solrCore");
							indexScheduler.getContext().put("solrCore", DefaultSlaveService.this.solrCore);
						} catch (SchedulerException e) {
							logger.error(e,e);
						}
					}
				} catch(Exception e){
					swapSuc = false;
					logger.error("切换Core失败",e);
				} 
				
				if(swapSuc){
					logger.warn("Core切换成功，重写增量开始时间文件 ==> " + incrDate);
					IndexFileUtils.writeIncrStartTimeToFile(incrXmlSourceDir, incrDate);
				}
				
				logger.warn("本机(Slave) 从Master机器上拉全量索引文件完毕，告诉Master机器这个振奋的消息.");
				try {
					masterService.pullIndexFinished(localIp + " 机器同步全量索引文件完毕，成功.");
				} catch (TerminatorMasterServiceException e) {
					logger.warn("本机(Slave) 从Master机器上拉全量索引文件完毕，通知Master机器失败.",e);
				}
			}else{
				logger.warn("本机(Slave)从Master机器上拉全量索引文件失败,不做SolrCore的切换,并将这个噩耗告知Master，让其不要继续等待.失败的文件名为 ==> " + pendingList.toArray());
				try {
					masterService.pullIndexFinished(localIp + " 机器同步全量索引文件失败!!!.");
				} catch (TerminatorMasterServiceException e) {
					logger.error("本机(Slave) 从Master机器上拉全量索引文件完毕，通知Master机器失败.",e);
				}
			}
		}

	}
	
	private void initIsSlave(){
		String coreName = this.solrCore.getName();
		CoreProperties coreProperties = null;
		try {
			coreProperties = new CoreProperties(solrCore.getResourceLoader().openConfig("core.properties"));
		} catch (IOException e) {
			logger.warn("读取 [" + coreName +"] 的core.properties文件失败",e);
			return;
		}
		
		this.isSlave = coreProperties != null && !coreProperties.isWriter();
	}

	/**
	 * Master需要订阅Master机器发布的服务
	 */
	private void  subscribeHsf(){
		String coreName = solrCore.getName();
		logger.warn("本机器对应于  " + coreName + "  是Slave角色,故需要订阅相对应的Master发布的内部通讯服务.");
		String[] ss = TerminatorCommonUtils.splitCoreName(coreName);
		if(ss == null){
			logger.error("CoreName ==> " + coreName + " 分解成serviceName groupName失败.");
		}
		String hsfVersion = TerminatorHSFContainer.Utils.genearteVersion(ServiceType.writer, coreName);
		try {
			masterService = (MasterService)TerminatorHSFContainer.subscribeService(MasterService.class.getName(), hsfVersion).getObject();
		} catch (TerminatorHsfSubException e) {
			logger.error(e,e);
		} catch (Exception e) {
			logger.error(e,e);
		}
	}
	
	private void publishHsf(){
		String coreName = this.solrCore.getName();
		logger.warn("发布对应于  " + coreName +"  的HSF服务.");
		String localIp = TerminatorCommonUtils.getLocalHostIP();
		String hsfVersion = TerminatorHSFContainer.Utils.generateSlaveWriteService(coreName, localIp);
		logger.warn("本机器对应于  " + coreName + "  是Slave角色，故须对Master端对外暴露相关服务.");
		logger.warn("发布  HsfVerison ==> " + hsfVersion);
		try {
			TerminatorHSFContainer.publishService(this, SlaveService.class.getName(), hsfVersion);
		} catch (TerminatorHsfPubException e) {
			logger.error(e,e);
		}
	}
	
	private void initFileSearcher(){
		this.incrIndexFileSearcher = new IncrIndexFileSearcher(this.incrXmlSourceDir);
	}
	
	/**
	 * 初始化存放xml数据的目录，保存在solr home目录下面，在做core切换的时候不需要更改。
	 */
	private void createXmlDataDir(){
		File incrDir = new File(this.solrCore.getCoreDescriptor().getCoreContainer().getSolrHome()+ File.separator + this.solrCore.getName() + File.separator + "incr_xml_source");
		if(!incrDir.exists()){
			incrDir.mkdir();
		}
		this.incrXmlSourceDir = incrDir;
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
