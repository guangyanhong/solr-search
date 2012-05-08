package com.taobao.terminator.core.realtime.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.utils.HSFUtils;
import com.taobao.terminator.core.realtime.BuildIndexJob;
import com.taobao.terminator.core.realtime.SelfPublisher;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;
import com.taobao.terminator.core.realtime.service.SyncIndexFilesJob.AfterSync;

/**
 * Follower角色组内通讯服务的默认实现
 * 
 * @author yusen
 *
 */
public class DefaultFollowerService implements FollowerService,SelfPublisher{
	private final Log log = LogFactory.getLog(DefaultFollowerService.class);
	
	private String coreName;
	private CoreContainer coreContainer;
	private LeaderService leaderService;
	private CommitLogAccessor commitLogAccessor;
	private BuildIndexJob indexBuilderJob;
	
	public DefaultFollowerService(CoreContainer coreContainer,String coreName,CommitLogAccessor commitLogAccessor,BuildIndexJob indexBuilderJob) {
		this.coreContainer = coreContainer;
		this.coreName = coreName;
		this.commitLogAccessor = commitLogAccessor;
		this.indexBuilderJob = indexBuilderJob;
	}
	
	@Override
	public void publishHsfService(String coreName) {
		String version = Utils.genHsfVersion(coreName);
		try {
			HSFUtils.publish(FollowerService.class.getName(),version, this);
			HSFUtils.publishCSData(Utils.genCSDataId(coreName), version, Utils.DEFAULT_HSF_GROUP);
		} catch (Exception e) {
			throw new RuntimeException("Pubish FollowerService ERROR!",e);
		}
	}
	
	public LeaderService subscribeLeaderService() {
		try {
            return null;
			//return leaderService = (LeaderService)HSFUtils.subscribe(LeaderService.class.getName(), LeaderService.Utils.genHsfVersion(coreName)).getObject();
		} catch (Exception e) {
			throw new RuntimeException("Subscribe LeaderService ERROR!", e);
		}
	}
	
	@Override
	public int notifyFollower(String ip, int port, String[] fileNames,SegmentPoint fullPoint) {
		log.warn("Leader Notify me! & fetch index files from leader.");
		SyncIndexFilesJob job = this.newJob(ip, port, fileNames,fullPoint);
		Thread t = new Thread(job,"SYNC-INDEX-FILES-THREAD");
		t.start();
		return 1;
	}
	
	private SyncIndexFilesJob newJob(String ip, int port, String[] fileNames,SegmentPoint fullPoint) {
		SyncIndexFilesJob job = new SyncIndexFilesJob();
		job.setLeaderIp(ip);
		job.setFullPoint(fullPoint);
		job.setPort(port);
		job.setFileNames(fileNames);
		job.setCoreContainer(coreContainer);
		job.setCoreName(coreName);
		job.setAfterSync(new DefaultAfterSync());
		job.setCommitLogAccessor(commitLogAccessor);
		job.setIndexBuilderJob(indexBuilderJob);
		job.setExceptionHandler(null); //使用默认的记录日志的ExceptionHandler
		return job;
	}
	
	public class DefaultAfterSync implements AfterSync {
		@Override
		public void afterSync(boolean isSuc) {
			if(isSuc) {
				log.warn("Sync index files SUC! Report to Leader!!!!");
				leaderService.report(TerminatorCommonUtils.getLocalHostIP(),true, "OK");
			} else {
				log.warn("Sync index files ERROR! Report to Leader!!!!");
				leaderService.report(TerminatorCommonUtils.getLocalHostIP(),false, "FAILED");
			}
		}
	}
}
