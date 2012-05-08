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
 * Leader机器的主控启动类
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
		 * Leader的启动过程如下：
		 *  -1- 初始化CommitLogAccessor,Master模式启动
		 *  -2- 初始化CommitLogSyncServer
		 *  -3- 发布LeaderService
		 *  -4- 订阅组内的FollowerService
		 *  -4- 初始化(发布)RealTimeService服务，并将其自身发布为HSF服务,>>>>> 至此客户端可以调用实时写的服务
		 *  
		 *  -5- 创建读CommitLog的任务，从CommitLog文件中不断读取实时的Reques，并将其作用于实时索引
		 *  -6- 启动全量Dump的任务，并将该任务加入Scheduler进行时间调度
		 *  -7- 启动搜索服务
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
		int backStep = solrConfig.getInt("commitLogArgs/recoverBackStep",1); //配置的回退步数
		
		String serializerClass = solrConfig.get("commitLogArgs/serializer","DEFAULT");
		Serializer serializer = null;
		if(!serializerClass.equals("DEFAULT")) {
			serializer = (Serializer)this.solrResourceLoader.newInstance(serializerClass, (String)null);
		}
		
		int backStep2 = this.getBackStep(); //计算得到的回退步数
		
		//计算的值大于配置的回退步数的话，就已计算的为准，否则以配置的为准
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
	 * 计算回复内存索引的回退步数,并将有问题的索引目录删除掉
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
				//删除有问题的
				if(!indexDir.delete()) {
					throw new RuntimeException("Delete index-dir ERROR,dir {" + indexDir.getAbsolutePath() + "}");
				}
			}
		}
		return indexs.size() - i;
	}
	
	/**
	 * 启动同步索引文件的Server
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
	 * 启动CommitLog同步的Server
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
	 * 将CommitLogAccessor对象注入到RealTimeUpdateHandler
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
	 * 发布组内的LeaderService的HSF服务
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
	 * 发布接受实时请求的HSF服务
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
	 * 启动从CommitLog读数据并构建实时索引的Job
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
	 * 初始化全量dump的Job和时间任务
	 */
	private void initFullDumpJob() {
		SolrResourceLoader resourceLoader = solrCore.getResourceLoader();
		
		String springConfig = solrConfig.get("dumpArgs/springConfigs", "applicationContext.xml");
		if(StringUtils.isBlank(springConfig)){
			throw new InitException("[" + coreName + "]" + "没有配置Service参数 ==> " + TerminatorContextLoader.SPRING_CONFIGS + ",故不能加载Spring容器.");
		}
		
		log.warn("[" + coreName + "] Spring配置文件为 ==> " + springConfig);
		String[] springConfigArray = springConfig.split(",");
		List<String> springConfigs = new ArrayList<String>(springConfigArray.length);
		for(String config : springConfigArray){
			springConfigs.add(config);
		}
		
		String confDir = resourceLoader.getConfigDir();
		
		URLClassLoader urlClassLoader = null;
		try {
			Field field = resourceLoader.getClass().getDeclaredField("classLoader"); //利用反射访问SolrResourceLoader的private classLoader
			field.setAccessible(true);
			urlClassLoader = (URLClassLoader)field.get(resourceLoader);
		} catch (Exception e) {
			throw new InitException("[" + coreName + "]" + "[loadSpringContext] --> 加载Spring容器是反射获取SolrResourceLoader对象的private属性classLoader失败",e);
		}
		
		Map<String, Object> properties = new ContextInfo(solrCore);
		TerminatorContextLoader terminatorContextLoader = new TerminatorContextLoader(confDir, springConfigs, properties, urlClassLoader);
		
		try {
			terminatorContextLoader.init();
		} catch(Exception e) {
			throw new RuntimeException("[" + coreName + "]" + "[loadSpringContext] --> TerminatorContextLoader加载Spring容器失败",e);
		}
		
		this.applicationContext = terminatorContextLoader.getApplicationContext();
		
		this.fullIndexProvider = new FullIndexProvider();
		
		this.fullIndexProvider.setDataProvider((DataProvider)this.applicationContext.getBean("fullDataProvider"));
		this.fullIndexProvider.setSolrCore(solrCore);
		
		try {
			this.fullTimer = (FullTimer)this.applicationContext.getBean("fullTimer");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "用户没有定义FullTimer,用默认的当前时间的FullTimer对象.");
		}
		
		DataProcessor processors = null;
		try {
			processors = (DataProcessor)this.applicationContext.getBean("dataProcessor");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "用户没有定义DataProcessor");
		}
		
		if(processors != null) {
			this.fullIndexProvider.setDataProcessor(processors);
		}
		
		GroupFilter groupFilter = null;
		try {
			groupFilter = (GroupFilter)this.applicationContext.getBean("groupFilter");
		} catch(Exception e) {
			log.warn("[" + coreName + "]" + "用户没有定义GroupFilter");
		}
		if(groupFilter != null) {
			this.fullIndexProvider.setGroupFilter(groupFilter);
		}
		//TODO
		this.fullDumpJob = new FullDumpJob(coreName,coreContainer, commitLogAccessor, fullIndexProvider, indexBuilderJob,leaderService,null,indexSyncServer,fullTimer);
		this.fullDumpJob.setExceptionHandler(new JobExceptionHandler() {
			@Override
			public void handleException(Thread thread, Throwable e) {
				log.error("全量Dumnp的任务抛出了异常，线程名称为 {" + thread.getName() +"},此次全量以失败告终.",e);
			}
		});
		
		String expr = solrConfig.get("dumpArgs/fullDumpTime");

		log.warn("[" + coreName + "] 全量周期为 ==> {" + expr +"}");
		TimerExpressions tes  = new TimerExpressions(urlClassLoader);
		TimerInfo ti = null;
		try {
			ti = tes.parse(expr);
		} catch (TimerExpressionException e1) {
			throw new RuntimeException("时间表达式解析错误",e1);
		}
		
		schedulerService = TerminatorSchedulerFactory.newScheduler();
		schedulerService.scheduleAtFixedRate(fullDumpJob, ti.initDelay, ti.period, ti.timeUnit);
	}
	
	public void fullDump() {
		log.warn("手动外部触发全量Dump..");
		fullDumpJob.run();
		log.warn("手动外部出发的全量Dump结束!");
	}
}
