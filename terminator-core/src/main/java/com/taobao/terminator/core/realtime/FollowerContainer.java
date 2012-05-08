package com.taobao.terminator.core.realtime;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.update.UpdateHandler;

import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogSyncClient;
import com.taobao.terminator.core.realtime.commitlog2.Serializer;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogSyncClient.AddressFetcher;
import com.taobao.terminator.core.realtime.service.DefaultFollowerService;
import com.taobao.terminator.core.realtime.service.LeaderService;

/**
 * Follower角色的主控启动类
 * 
 * @author yusen
 *
 */
public class FollowerContainer {
	private final Log log = LogFactory.getLog(FollowerContainer.class);
	 
	private String                 coreName;
	private SolrCore               solrCore;
	private CoreContainer          coreContainer;
	private SolrResourceLoader     solrResourceLoader;
	private SolrConfig             solrConfig;
	
	private DefaultSearchService   searchService;
	private CommitLogAccessor      commitLogAccessor;
	private File                   commitLogDir;
	
	private BuildIndexJob          indexBuilderJob;
	private CommitLogSyncClient    clSyncClient;
	
	private DefaultFollowerService followerService;
	private LeaderService          leaderService;
	
	public FollowerContainer(CoreContainer coreContainer,String coreName) {
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
		 *  -1- 初始化CommitLogAccessor，Slave模式启动
		 *  -2- 初始化CommitLogSyncClient
		 *  -3- 发布FollowerService
		 *  
		 *  -5- 创建读CommitLog的任务，从CommitLog文件中不断读取实时的Reques，并将其作用于实时索引
		 *  -6- 启动全量Dump的任务，并将该任务加入Scheduler进行时间调度
		 *  -7- 启动搜索服务
		 * */
		
		this.initCommitLogAccessor();
		
		this.rejectCommitLogAccessor();

		this.initIndexBuilderJob();
		
		this.publishFollowerService();
		
		/* 该方法会阻塞，直到LeaderService的IP列表推动过来，因为CommigLogSyncClient的启动需要调用LeaderService的方法 */
		this.subscribeLeaderService();
		
		this.startCommitLogSyncClient();
		
		this.publishSearchService();
	}
	
	private void initCommitLogAccessor() {
		String instanceDir = solrCore.getResourceLoader().getInstanceDir();
		this.commitLogDir = new File(instanceDir, "commitlogs");
		if (!commitLogDir.exists()) {
			commitLogDir.mkdirs();
		}
		
		int sizeOfSegment = solrConfig.getInt("commitLogArgs/sizeOfSegment");
		int backStep = solrConfig.getInt("commitLogArgs/recoverBackStep",1);
		
		String serializerClass = solrConfig.get("commitLogArgs/serializer","DEFAULT");
		Serializer serializer = null;
		if(!serializerClass.equals("DEFAULT")) {
			serializer = (Serializer)this.solrResourceLoader.newInstance(serializerClass, (String)null);
		}
		
		try {
			this.commitLogAccessor = new CommitLogAccessor(this.commitLogDir, sizeOfSegment, serializer, backStep, false);
		} catch (Exception e) {
			throw new RuntimeException("Create CommitLogAccessor ERROR!", e);
		}
	}
	
	private void initIndexBuilderJob() {
		indexBuilderJob = new BuildIndexJob(this.commitLogAccessor,solrCore.getUpdateHandler(),solrCore.getSchema());
		new Thread(indexBuilderJob,"BUILD-INDEX-JOB").start();
	}

	private void publishFollowerService() {
		followerService = new DefaultFollowerService(coreContainer, coreName,commitLogAccessor,indexBuilderJob);
		followerService.publishHsfService(coreName);
	}
	
	private void rejectCommitLogAccessor() {
		UpdateHandler updateHandler = this.solrCore.getUpdateHandler();
		if(updateHandler instanceof TerminatorUpdateHandler) {
			((TerminatorUpdateHandler)updateHandler).getRealTimeUpdateHandler().setCommitLogAccessor(commitLogAccessor);
		}
	}
	
	private void subscribeLeaderService() {
		this.leaderService = followerService.subscribeLeaderService();
	}
	
	private void startCommitLogSyncClient() {
		new Thread(new StartCLSyncClientJob(),"START-COMMIGLOG-SYNC-CLIENT-THREAD").start();
	}
	
	private class StartCLSyncClientJob implements Runnable {
		@Override
		public void run() {
			try {
				 clSyncClient = new CommitLogSyncClient(commitLogDir, new LeaderAddressFetcher(), 500);
				 clSyncClient.start();
			} catch (Exception e) {
				throw new RuntimeException("[FOLLOWER] - Start CommitLogSyncClient ERROR!", e);
			}
		}
	}
	
	class LeaderAddressFetcher implements AddressFetcher {
		@Override
		public Address fetch() {
			return leaderService.getCLSyncAdd();
		}
	}
	
	private void publishSearchService() {
		searchService = new DefaultSearchService(solrCore);
		searchService.publishHsfService(coreName);
	}
}
