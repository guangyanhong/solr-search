package com.taobao.terminator.core.realtime;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.common.stream.FileGetServer;
import com.taobao.terminator.common.timers.Job;
import com.taobao.terminator.core.dump.FullIndexProvider;
import com.taobao.terminator.core.index.stream.FullIndexFileProvider;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogAccessor;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;
import com.taobao.terminator.core.realtime.service.DefaultLeaderService;
import com.taobao.terminator.core.realtime.service.SolrUtils;

/**
 * 全量Dump的流程控制权在这个类里，同时此类实现了InnerService接口，作为内部协调的实现类
 * 
 * @author yusen
 */
public class FullDumpJob extends Job {
	private final Log log = LogFactory.getLog(FullDumpJob.class);

	private String               coreName;
	private CoreContainer        coreContainer;

	private CommitLogAccessor    commitLogAccessor;
	
	private FullIndexProvider    fullIndexProvider;
	
	private BuildIndexJob        buildIndexJob;
	
	private DefaultLeaderService leaderService;
    //TODO
	//private FollowersAccessor    followersAccessor;
	
	private FileGetServer        indexSyncServer;
	private FullTimer            fullTimer;
	
	public FullDumpJob(String coreName,
					  CoreContainer coreContainer, 
					  CommitLogAccessor commitLogAccessor, 
					  FullIndexProvider fullIndexProvider, 
					  BuildIndexJob buildIndexJob,
					  DefaultLeaderService leaderService,
					  FileGetServer indexSyncServer,
					  FullTimer fullTimer) {
		
		this.coreName = coreName;
		this.commitLogAccessor = commitLogAccessor;
		this.coreContainer = coreContainer;
		this.fullIndexProvider = fullIndexProvider;
		this.buildIndexJob = buildIndexJob;
		this.leaderService = leaderService;
		//this.followersAccessor = followersAccessor;
		this.indexSyncServer = indexSyncServer;
		this.fullTimer = fullTimer;
	}

	@SuppressWarnings("unchecked")
	public void doJob() throws Exception {
		SolrCore rSolrCore = this.coreContainer.getCore(this.coreName); //设置回滚的SolrCore'
		String rDataDir = rSolrCore.getDataDir();
		String newDataDir = null;
		if(rDataDir != null) {
			try {
				SolrCore newCore = SolrUtils.createNewCore(coreContainer,coreName);
				newDataDir = newCore.getDataDir();
				log.warn(">>>>> Full-Dump <<<<<  START,create new SolrCore,data dir is {" + newCore.getDataDir() +"}");
				
				/* 设定当前的Full-Dump的目标SolrCore */
				fullIndexProvider.setSolrCore(newCore);
				log.warn(">>>>> Full-Dump <<<<<  Current SolrCore's UpdateHandler is DIRECT MODE.");
				TerminatorUpdateHandler newUpdateHandler = (TerminatorUpdateHandler) newCore.getUpdateHandler();
				newUpdateHandler.switchMode(TerminatorUpdateHandler.MODE_DIRECT);
				newUpdateHandler.getRealTimeUpdateHandler().setCommitLogAccessor(commitLogAccessor);
				
				FullTimer currentTimeFullTimer = null;
				if(fullTimer == null) {
					long now = System.currentTimeMillis();
					log.warn(">>>>> Full-Dump <<<<<  fullTimer is CurrentTimeFullTimer --> " + now);
					currentTimeFullTimer = new CurrentTimeFullTimer(now);
				}
				
				log.warn(">>>>> Full-Dump <<<<<  Full Dumping... ");
				/* * 索引Dump过程 * *
				 * fww添加,dump失败后，清除新索引路径下所有无用的索引文件
				 * */
				try {
					fullIndexProvider.dump();
				} catch (Exception e) {
					if(!StringUtils.isBlank(newDataDir)) {
						FileUtils.cleanDirectory(new File(newDataDir));
						newDataDir = null;
					}
					this.coreContainer.getCore(this.coreName).getCoreDescriptor().setDataDir(rDataDir);
					throw new RuntimeException("全量dump过程出现异常，全量dump失败", e);
				}
				/* *
				 * 全量Dump后的一段时间存在newCore和oldCore并存的局面，此时先通知Follower机器来Leader机器复制Index文件
				 * 这期间oldCore在提供对外的服务，此期间实时的请求仍然作用于oldCore上，Follower的复制工作全部完成之后，
				 * 进行oldCore <---> newCore的交换工作，在交换core之前进行一些处理，如下代码：
				 * */
				//int followerCount = followersAccessor.getFollowerCount();
				//log.warn(">>>>> Full-Dump <<<<<  Dump-OK notify followers. FollowerCount is {"  + followerCount +"}");
				//CountDownLatch latch = new CountDownLatch(followerCount);
				//followersAccessor.registerLatch(latch);
				Address indexSyncAdd = leaderService.getIndexSyncAdd();
				
				if(!indexSyncServer.isAlive()) {
					indexSyncServer.start();
				}
				String indexDir = newCore.getDataDir() + "/index";
				FullIndexFileProvider provider = new FullIndexFileProvider(new File(indexDir));
				indexSyncServer.register(FullIndexFileProvider.type, provider);
				
				long fullTime = 0L;
				if(fullTimer != null) {
					// 清理掉多余的Segment文件
					try {
						fullTime = fullTimer.getTime();
					} catch (Throwable e) {
						log.error("用系统配置的FullTimer获取时间异常,为了保证系统仍然能够运行，使用全量前的时间做为fullTime(由此可能会产生数据不一致的问题.)",e);
						fullTime = currentTimeFullTimer.getTime();
					}
				} else {
					fullTime = currentTimeFullTimer.getTime();
				}
				
				buildIndexJob.pause();
				
				//获取全量后的数据恢复点
				SegmentPoint p = commitLogAccessor.getNearCheckPoint(fullTime);
				//TODO
				//int sucCount = followersAccessor.notifyFollower(indexSyncAdd.getIp(), indexSyncAdd.getPort(), this.getIndexFiles(newCore),p);
				//log.warn(">>>>> Full-Dump <<<<< Notify followers suncCount is {" + sucCount +"}");
				
				//leaderService.registerLatch(latch);
				log.warn(">>>>> Full-Dump <<<<< waiting followers...");
				
				//latch.await(1 * 60 * 60 * 1000,TimeUnit.MILLISECONDS);
				leaderService.resetLatch();
				
				
				/* *
				 * 所有的Follower机器复制Leader的索引文件成功之后，进行如下操作:
				 * -1- 将newCore的UpdateHandler切换成REALTIME模式
				 * -2- 调用newCore的getSearcher()方法(IndexReader.reopen())
				 * -3- 暂停一下写CommitLog的线程和读CommitLog构建索引的Index-Builder线程
				 * -4- 通过给的时间点，定位到fullAt点 ，并将fullAt之前的所有的Segment删除掉
				 * -5- CommitLog的Reader定位到fullAt点
				 * -6-  恢复CommitLog的写线程和Index-Builder的线程，Index-Builder线程从fullAt点开始读取CommitLog，回复全量期间的实时数据
				 * -7- 将newCore注册到CoreContainer，此时对于Search服务来说，所有的Search请求会请求到newCore的的索引
				 * -8- 关闭oldCore，至此整个Full-Dump过程全部结束
				 * */
				log.warn(">>>>> Full-Dump <<<<<  Switch MODE to REALTIME_MODE.");
				newUpdateHandler.switchMode(TerminatorUpdateHandler.MODE_REALTIME);
				IndexReaderFactory indexReaderFactory = newCore.getIndexReaderFactory();
				if (indexReaderFactory instanceof RealTimeIndexReaderFactory) {
					((RealTimeIndexReaderFactory) indexReaderFactory).getIsAfterFull().set(true);
				}
				
				log.warn(">>>>> Full-Dump <<<<<  getSearcher(true,false,futures)");
				Future[] futures = new Future[1];
				newCore.getSearcher(true, false, futures);
				futures[0].get();
				
				//清理多余的一些Segment文件，并重新设置Reader的指针到全量恢复点
				log.warn(">>>>> Full-Dump <<<<<  Clear & Reset Segment Files.");
				commitLogAccessor.clearAndReset(p);
				
				//BuildIndexJob的目标索引指向新的UpdateHandler
				buildIndexJob.setUpdateHandler(newUpdateHandler);
				
				//将newCore注册到CoreContainer，注册之后新的Search就会搜索这个新的SolrCore了
				SolrCore oldCore = coreContainer.register(newCore, true);
				coreContainer.persist();
				
				
				try {
					/*****临时一个解决方案,强制性绕开引用计数器，直接关闭掉相应的IndexReader*****/ //FIXME
					IndexReaderFactory fac = oldCore.getIndexReaderFactory();
					if(fac instanceof RealTimeIndexReaderFactory) {
						RealTimeIndexReaderFactory rtFac = (RealTimeIndexReaderFactory)fac;
						rtFac.getMainReader().setForceDecRef();
						
						List<TerminatorIndexReader>  rl = rtFac.getDiskReaders();
						if(rl != null && !rl.isEmpty()) {
							for(TerminatorIndexReader r : rl) {
								r.setForceDecRef();
							}
						}
					}
				} catch (Exception e) {
					log.error(e);
				}
				
				if (oldCore != null) {
					do {
						oldCore.close();
					} while (!oldCore.isClosed());
				}
				
				//清理并关闭掉老的SolrCore对象
				log.warn(">>>>> Full-Dump <<<<< Clean old DataDir... oldCore索引路径--->>>" + oldCore.getDataDir());
				FileUtils.cleanDirectory(new File(oldCore.getDataDir()));
				
			}
			finally  {
				rDataDir = null;
				newDataDir = null;
				Thread.sleep(3000);
				buildIndexJob.resume();
			}
		} else {
			log.error("获取oldCore的索引地址失败，此次全量dump失败");
		}
	}

	private String[] getIndexFiles(SolrCore core) {
		File indexDir = new File(core.getDataDir() + "index/");
		return indexDir.list();
	}
	
	public class CurrentTimeFullTimer implements FullTimer {
		private long time;
		
		public CurrentTimeFullTimer(long time) {
			this.time = time;
		}

		@Override
		public long getTime() {
			return time;
		}
	}
}
