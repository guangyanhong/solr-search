package com.taobao.terminator.core.realtime;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.update.UpdateHandler;
import org.springframework.context.ApplicationContext;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.data.DataProvider;
import com.taobao.terminator.common.data.filter.GroupFilter;
import com.taobao.terminator.common.data.processor.DataProcessor;
import com.taobao.terminator.common.protocol.RealTimeService;
import com.taobao.terminator.common.stream.FileGetServer;
import com.taobao.terminator.common.timers.TerminatorSchedulerFactory;
import com.taobao.terminator.common.timers.TimerExpressionException;
import com.taobao.terminator.common.timers.TimerExpressions;
import com.taobao.terminator.common.timers.TimerInfo;
import com.taobao.terminator.common.timers.Job.JobExceptionHandler;
import com.taobao.terminator.common.utils.HSFUtils;
import com.taobao.terminator.core.ContextInfo;
import com.taobao.terminator.core.dump.FullIndexProvider;
import com.taobao.terminator.core.dump.InitException;
import com.taobao.terminator.core.dump.TerminatorContextLoader;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogSyncServer;
import com.taobao.terminator.core.realtime.commitlog2.Serializer;
import com.taobao.terminator.core.realtime.common.Utils;
import com.taobao.terminator.core.realtime.service.DefaultLeaderService;
import com.taobao.terminator.core.realtime.service.LeaderService;

/**
 * Leader����������������
 * 
 * @author yusen
 *
 */
public class LeaderContainer {
	private final Log log = LogFactory.getLog(Bootstraper3.class);
	 
	private String                 coreName;
	private SolrCore               solrCore;
	private CoreContainer          coreContainer;
	private SolrConfig             solrConfig;
	private SolrResourceLoader     solrResourceLoader;
	
	private DefaultRealTimeService realTimeService;
	private DefaultSearchService   searchService;
	private DefaultLeaderService   leaderService;
	
	private File                   commitLogDir;
	private CommitLogAccessor      commitLogAccessor;
	private ApplicationContext     applicationContext;
	
	private BuildIndexJob          indexBuilderJob;
	private FullDumpJob            fullDumpJob;
	
	private CommitLogSyncServer    clSyncServer;
	private FileGetServer          indexSyncServer ;
	
	private FullIndexProvider 	   fullIndexProvider;
	private ScheduledThreadPoolExecutor schedulerService ;
	
	//private FollowersAccessor followersAccessor;
	private FullTimer         fullTimer;
	
	public LeaderContainer(CoreContainer coreContainer,String coreName) {
		this.coreContainer = coreContainer;
		this.solrCore = coreContainer.getCore(coreName);
		this.coreName = solrCore.getName();
		this.solrConfig = solrCore.getSolrConfig();
		this.solrResourceLoader = solrCore.getResourceLoader();
		this.init();
	}

	protected void init() {
		/* *
		 * Leader�������������£�
		 *  -1- ��ʼ��CommitLogAccessor,Masterģʽ����
		 *  -2- ��ʼ��CommitLogSyncServer
		 *  -3- ����LeaderService
		 *  -4- �������ڵ�FollowerService
		 *  -4- ��ʼ��(����)RealTimeService���񣬲�����������ΪHSF����,>>>>> ���˿ͻ��˿��Ե���ʵʱд�ķ���
		 *  
		 *  -5- ������CommitLog�����񣬴�CommitLog�ļ��в��϶�ȡʵʱ��Reques��������������ʵʱ����
		 *  -6- ����ȫ��Dump�����񣬲������������Scheduler����ʱ�����
		 *  -7- ������������
		 * */
		
		
		this.initCommitLogAccessor();
		this.rejectCommitLogAccessor();
		this.startCommitLogSyncServer(); 
		this.startIndexSyncServer();
		
		this.publishLeaderService();
		this.initFollowersAccessor();
		this.publishRealTimeService();

		this.startIndexBuilderJob();
		
		this.initFullDumpJob();

		this.publishSearchService();
	}
	
	private void initFollowersAccessor() {
		//TODO
		//followersAccessor = new FollowersAccessor(coreName);
	}

	private void initCommitLogAccessor() {
		String instanceDir = solrCore.getResourceLoader().getInstanceDir();
		this.commitLogDir = new File(instanceDir, "commitlogs");
		if (!commitLogDir.exists()) {
			commitLogDir.mkdirs();
		}
		
		int sizeOfSegment = solrConfig.getInt("commitLogArgs/sizeOfSegment");
		int backStep = solrConfig.getInt("commitLogArgs/recoverBackStep",1); //���õĻ��˲���
		
		String serializerClass = solrConfig.get("commitLogArgs/serializer","DEFAULT");
		Serializer serializer = null;
		if(!serializerClass.equals("DEFAULT")) {
			serializer = (Serializer)this.solrResourceLoader.newInstance(serializerClass, (String)null);
		}
		
		int backStep2 = this.getBackStep(); //����õ��Ļ��˲���
		
		//�����ֵ�������õĻ��˲����Ļ������Ѽ����Ϊ׼�����������õ�Ϊ׼
		if(backStep < backStep2) {
			backStep = backStep2;
		}
		
		try {
			this.commitLogAccessor = new CommitLogAccessor(this.commitLogDir, sizeOfSegment, serializer, backStep, true);
		} catch (Exception e) {
			throw new RuntimeException("Create CommitLogAccessor ERROR!", e);
		}
	}
	
	/**
	 * ����ظ��ڴ������Ļ��˲���,���������������Ŀ¼ɾ����
	 * @return
	 */
	private int getBackStep() {
		File dataDir = new File(solrCore.getDataDir());
		List<File> indexs = Utils.listIndexDirs(dataDir);
		int  i = indexs.size() -1 ;
		for(;i >= 0;i--) {
			File indexDir = indexs.get(i);
			if(Utils.isProperIndex(indexDir)) {
				break;
			} else {
				//ɾ���������
				if(!indexDir.delete()) {
					throw new RuntimeException("Delete index-dir ERROR,dir {" + indexDir.getAbsolutePath() + "}");
				}
			}
		}
		return indexs.size() - i;
	}
	
	/**
	 * ����ͬ�������ļ���Server
	 */
	private void startIndexSyncServer() {
		String host = solrConfig.get("indexSyncArgs/host", "{!localhost}");
		int port = solrConfig.getInt("indexSyncArgs/port", 12508);

		if (host.equals("{!localhost}")) {
			host = TerminatorCommonUtils.getLocalHostIP();
		}

		this.indexSyncServer = new FileGetServer(host, port);

		try {
			this.indexSyncServer.start();
		} catch (Exception e) {
			throw new RuntimeException("Start FileGetServer Error!", e);
		}
	}
	
	/**
	 * ����CommitLogͬ����Server
	 */
	private void startCommitLogSyncServer() {
		int port = solrConfig.getInt("commitLogArgs/syncServer/port", 12345);
		String host = solrConfig.get("commitLogArgs/syncServer/host", "{!localhost}");

		if (host.equals("{!localhost}")) {
			host = TerminatorCommonUtils.getLocalHostIP();
		}

		int threadPoolSize = solrConfig.getInt("commitLogArgs/syncServer/threadPoolSize", 10);

		try {
			this.clSyncServer = new CommitLogSyncServer(commitLogDir, host, port, threadPoolSize);
			this.clSyncServer.start();
		} catch (Exception e) {
			throw new RuntimeException("[MASTER] - Start CommitLogSyncServer ERROR", e);
		}
	}
	
	/**
	 * ��CommitLogAccessor����ע�뵽RealTimeUpdateHandler
	 */
	private void rejectCommitLogAccessor() {
		UpdateHandler updateHandler = this.solrCore.getUpdateHandler();
		if(updateHandler instanceof TerminatorUpdateHandler) {
			((TerminatorUpdateHandler)updateHandler).getRealTimeUpdateHandler().setCommitLogAccessor(commitLogAccessor);
		} else {
			throw new RuntimeException("UpdateHalder is not instance of 'TerminatorUpdateHandler'!!");
		}
	}
	
	/**
	 * �������ڵ�LeaderService��HSF����
	 */
	private void publishLeaderService() {
		leaderService = new DefaultLeaderService(clSyncServer,indexSyncServer);
		try {
			HSFUtils.publish(LeaderService.class.getName(), LeaderService.Utils.genHsfVersion(coreName), leaderService);
		} catch (Exception e) {
			throw new RuntimeException("Publish LeaderService ERROR!",e);
		}
	}
	
	/**
	 * ��������ʵʱ�����HSF����
	 */
	private void publishRealTimeService() {
		realTimeService = new DefaultRealTimeService(commitLogAccessor);
		try {
			HSFUtils.publish(RealTimeService.class.getName(), RealTimeService.Utils.genHsfVersion(coreName), realTimeService);
		} catch (Exception e) {
			throw new RuntimeException("Publish RealTimeService ERROR!",e);
		}
	}
	
	/**
	 * ������CommitLog�����ݲ�����ʵʱ������Job
	 */
	private void startIndexBuilderJob() {
		indexBuilderJob = new BuildIndexJob(this.commitLogAccessor,solrCore.getUpdateHandler(),solrCore.getSchema());
		new Thread(indexBuilderJob,"BUILD-INDEX-JOB").start();
	}
	
	private void publishSearchService() {
		searchService = new DefaultSearchService(solrCore);
		searchService.publishHsfService(coreName);
	}
	
	/**
	 * ��ʼ��ȫ��dump��Job��ʱ������
	 */
	private void initFullDumpJob() {
		SolrResourceLoader resourceLoader = solrCore.getResourceLoader();
		
		String springConfig = solrConfig.get("dumpArgs/springConfigs", "applicationContext.xml");
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
			throw new RuntimeException("[" + coreName + "]" + "[loadSpringContext] --> TerminatorContextLoader����Spring����ʧ��",e);
		}
		
		this.applicationContext = terminatorContextLoader.getApplicationContext();
		
		this.fullIndexProvider = new FullIndexProvider();
		
		this.fullIndexProvider.setDataProvider((DataProvider)this.applicationContext.getBean("fullDataProvider"));
		this.fullIndexProvider.setSolrCore(solrCore);
		
		try {
			this.fullTimer = (FullTimer)this.applicationContext.getBean("fullTimer");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "�û�û�ж���FullTimer,��Ĭ�ϵĵ�ǰʱ���FullTimer����.");
		}
		
		DataProcessor processors = null;
		try {
			processors = (DataProcessor)this.applicationContext.getBean("dataProcessor");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "�û�û�ж���DataProcessor");
		}
		
		if(processors != null) {
			this.fullIndexProvider.setDataProcessor(processors);
		}
		
		GroupFilter groupFilter = null;
		try {
			groupFilter = (GroupFilter)this.applicationContext.getBean("groupFilter");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "�û�û�ж���GroupFilter");
		}
		if(groupFilter != null) {
			this.fullIndexProvider.setGroupFilter(groupFilter);
		}
		//TODO
		this.fullDumpJob = new FullDumpJob(coreName,coreContainer, commitLogAccessor, fullIndexProvider, indexBuilderJob,leaderService,null,indexSyncServer,fullTimer);
		this.fullDumpJob.setExceptionHandler(new JobExceptionHandler() {
			@Override
			public void handleException(Thread thread, Throwable e) {
				log.error("ȫ��Dumnp�������׳����쳣���߳�����Ϊ {" + thread.getName() +"},�˴�ȫ����ʧ�ܸ���.",e);
			}
		});
		
		String expr = solrConfig.get("dumpArgs/fullDumpTime");

		log.warn("[" + coreName + "] ȫ������Ϊ ==> {" + expr +"}");
		TimerExpressions tes  = new TimerExpressions(urlClassLoader);
		TimerInfo ti = null;
		try {
			ti = tes.parse(expr);
		} catch (TimerExpressionException e1) {
			throw new RuntimeException("ʱ����ʽ��������",e1);
		}
		
		schedulerService = TerminatorSchedulerFactory.newScheduler();
		schedulerService.scheduleAtFixedRate(fullDumpJob, ti.initDelay, ti.period, ti.timeUnit);
	}
	
	public void fullDump() {
		log.warn("�ֶ��ⲿ����ȫ��Dump..");
		fullDumpJob.run();
		log.warn("�ֶ��ⲿ������ȫ��Dump����!");
	}
}
