package com.taobao.terminator.core.realtime.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.common.stream.FileGetClient;
import com.taobao.terminator.common.stream.FileGetResponse;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.realtime.BuildIndexJob;
import com.taobao.terminator.core.realtime.RealTimeIndexReaderFactory;
import com.taobao.terminator.core.realtime.TerminatorUpdateHandler;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;

/**
 * Leader全量完毕之后Follower同步Index文件的任务
 * 
 * @author yusen
 * 
 */
public class SyncIndexFilesJob implements Runnable {
	private final Log log = LogFactory.getLog(SyncIndexFilesJob.class);
	
	private String coreName;
	private CoreContainer coreContainer;
	private String leaderIp;
	private int port;
	private String[] fileNames;
	private ExceptionHandler exceptionHandler = new LogExceptionHanlder(); // 同步期间出现异常的处理机制,默认是抛出RuntimeException，终止掉当前线程
	private AfterSync afterSync; // 同步完成后(无论成功与失败)做的事情，比如Core切换之类的
	private BuildIndexJob indexBuilderJob;
	private CommitLogAccessor commitLogAccessor;
	private SegmentPoint fullPoint;
	
	public SyncIndexFilesJob() { }

	public SyncIndexFilesJob(String coreName, 
							CoreContainer coreContainer, 
							String leaderIp, 
							int port, 
							String[] fileNames,
							ExceptionHandler exceptionHandler, 
							AfterSync afterSync, 
							BuildIndexJob indexBuilderJob, 
							CommitLogAccessor commitLogAccessor, 
							SegmentPoint fullPoint) {
		
		this.coreName = coreName;
		this.coreContainer = coreContainer;
		this.leaderIp = leaderIp;
		this.port = port;
		this.fileNames = fileNames;
		this.exceptionHandler = exceptionHandler;
		this.afterSync = afterSync;
		this.indexBuilderJob = indexBuilderJob;
		this.commitLogAccessor = commitLogAccessor;
		this.fullPoint = fullPoint;
	}

	@Override
	public void run() {
		try {
			doRun();
		} catch (Throwable e) {
			exceptionHandler.handle(e);
		}
	}

	private void doRun() throws Exception {
		SolrCore newCore = SolrUtils.createNewCore(coreContainer, coreName);
		File indexDir = this.prepareNewIndexDir(newCore);

		FileGetClient fileClient = new FileGetClient(leaderIp, port);
		boolean syncSuc = true;
		for (String name : fileNames) {
			FileOutputStream fileOutputStream = null;
			try {
				fileOutputStream = new FileOutputStream(new File(indexDir, name));
				int code = fileClient.doGetFile(FullIndexFileProvider.type, name, fileOutputStream);
				if (FileGetResponse.SUCCESS != code) {
					syncSuc = false;
				}
			} finally {
				if (fileOutputStream != null) {
					fileOutputStream.close();
				}
			}
		}
		
		if(syncSuc) { //同步索引文件有问题的话，暂时不管了，忽略掉这次全量吧，下次再说
			
			TerminatorUpdateHandler newUpdateHandler = (TerminatorUpdateHandler)newCore.getUpdateHandler();
			newUpdateHandler.switchMode(TerminatorUpdateHandler.MODE_REALTIME);
			IndexReaderFactory indexReaderFactory = newCore.getIndexReaderFactory();
			if (indexReaderFactory instanceof RealTimeIndexReaderFactory) {
				((RealTimeIndexReaderFactory) indexReaderFactory).getIsAfterFull().set(true);
			}
			newCore.getSearcher(true, false, null);
			
			indexBuilderJob.pause();
			
			commitLogAccessor.clearAndReset(fullPoint);
			indexBuilderJob.setUpdateHandler(newUpdateHandler);
			
			SolrCore oldCore = coreContainer.register(newCore, true);
			coreContainer.persist();
			
			log.warn(">>>>> Full-Dump <<<<< Clean old DataDir...");
			FileUtils.cleanDirectory(new File(oldCore.getDataDir()));
			
			if (oldCore != null) {
				do {
					oldCore.close();
				} while (!oldCore.isClosed());
			}
			
			indexBuilderJob.resume();
		}
		
		this.afterSync.afterSync(syncSuc);
	}

	private File prepareNewIndexDir(SolrCore newCore) {
		String dataDirStr = newCore.getDataDir();
		File dataDir = new File(dataDirStr);
		File indexDir = new File(dataDir, "index");

		if (indexDir.exists()) {
			try {
				FileUtils.cleanDirectory(indexDir);
			} catch (IOException e1) {
			}
		} else {
			indexDir.mkdirs();
		}

		return indexDir;
	}

	/**
	 * 全量任务过程当中出现异常的处理器
	 * 
	 * @author yusen
	 *
	 */
	public interface ExceptionHandler {
		public void handle(Throwable e);
	}

	/**
	 * 全量索引文件复制之后要做的事情
	 * 
	 * @author yusen
	 *
	 */
	public interface AfterSync {
		public void afterSync(boolean isSuc);
	}

	/**
	 * 默认的异常处理器 --- 记录错误日志
	 * 
	 * @author yusen
	 */
	public class LogExceptionHanlder implements ExceptionHandler {
		@Override
		public void handle(Throwable e) {
			log.error("Fetch index file from leader ERROR!",e);
		}
	}

	public String getLeaderIp() {
		return leaderIp;
	}

	public void setLeaderIp(String leaderIp) {
		this.leaderIp = leaderIp;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String[] getFileNames() {
		return fileNames;
	}

	public void setFileNames(String[] fileNames) {
		this.fileNames = fileNames;
	}

	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		if(exceptionHandler == null) 
			return;
		this.exceptionHandler = exceptionHandler;
	}

	public AfterSync getAfterSync() {
		return afterSync;
	}

	public void setAfterSync(AfterSync afterSync) {
		this.afterSync = afterSync;
	}

	public String getCoreName() {
		return coreName;
	}

	public void setCoreName(String coreName) {
		this.coreName = coreName;
	}

	public CoreContainer getCoreContainer() {
		return coreContainer;
	}

	public void setCoreContainer(CoreContainer coreContainer) {
		this.coreContainer = coreContainer;
	}

	public BuildIndexJob getIndexBuilderJob() {
		return indexBuilderJob;
	}

	public void setIndexBuilderJob(BuildIndexJob indexBuilderJob) {
		this.indexBuilderJob = indexBuilderJob;
	}

	public CommitLogAccessor getCommitLogAccessor() {
		return commitLogAccessor;
	}

	public void setCommitLogAccessor(CommitLogAccessor commitLogAccessor) {
		this.commitLogAccessor = commitLogAccessor;
	}

	public SegmentPoint getFullPoint() {
		return fullPoint;
	}

	public void setFullPoint(SegmentPoint fullPoint) {
		this.fullPoint = fullPoint;
	}
}
