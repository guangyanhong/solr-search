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
 * ȫ��Dump�����̿���Ȩ��������ͬʱ����ʵ����InnerService�ӿڣ���Ϊ�ڲ�Э����ʵ����
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
		SolrCore rSolrCore = this.coreContainer.getCore(this.coreName); //���ûع���SolrCore'
		String rDataDir = rSolrCore.getDataDir();
		String newDataDir = null;
		if(rDataDir != null) {
			try {
				SolrCore newCore = SolrUtils.createNewCore(coreContainer,coreName);
				newDataDir = newCore.getDataDir();
				log.warn(">>>>> Full-Dump <<<<<  START,create new SolrCore,data dir is {" + newCore.getDataDir() +"}");
				
				/* �趨��ǰ��Full-Dump��Ŀ��SolrCore */
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
				/* * ����Dump���� * *
				 * fww���,dumpʧ�ܺ����������·�����������õ������ļ�
				 * */
				try {
					fullIndexProvider.dump();
				} catch (Exception e) {
					if(!StringUtils.isBlank(newDataDir)) {
						FileUtils.cleanDirectory(new File(newDataDir));
						newDataDir = null;
					}
					this.coreContainer.getCore(this.coreName).getCoreDescriptor().setDataDir(rDataDir);
					throw new RuntimeException("ȫ��dump���̳����쳣��ȫ��dumpʧ��", e);
				}
				/* *
				 * ȫ��Dump���һ��ʱ�����newCore��oldCore����ľ��棬��ʱ��֪ͨFollower������Leader��������Index�ļ�
				 * ���ڼ�oldCore���ṩ����ķ��񣬴��ڼ�ʵʱ��������Ȼ������oldCore�ϣ�Follower�ĸ��ƹ���ȫ�����֮��
				 * ����oldCore <---> newCore�Ľ����������ڽ���core֮ǰ����һЩ�������´��룺
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
					// ����������Segment�ļ�
					try {
						fullTime = fullTimer.getTime();
					} catch (Throwable e) {
						log.error("��ϵͳ���õ�FullTimer��ȡʱ���쳣,Ϊ�˱�֤ϵͳ��Ȼ�ܹ����У�ʹ��ȫ��ǰ��ʱ����ΪfullTime(�ɴ˿��ܻ�������ݲ�һ�µ�����.)",e);
						fullTime = currentTimeFullTimer.getTime();
					}
				} else {
					fullTime = currentTimeFullTimer.getTime();
				}
				
				buildIndexJob.pause();
				
				//��ȡȫ��������ݻָ���
				SegmentPoint p = commitLogAccessor.getNearCheckPoint(fullTime);
				//TODO
				//int sucCount = followersAccessor.notifyFollower(indexSyncAdd.getIp(), indexSyncAdd.getPort(), this.getIndexFiles(newCore),p);
				//log.warn(">>>>> Full-Dump <<<<< Notify followers suncCount is {" + sucCount +"}");
				
				//leaderService.registerLatch(latch);
				log.warn(">>>>> Full-Dump <<<<< waiting followers...");
				
				//latch.await(1 * 60 * 60 * 1000,TimeUnit.MILLISECONDS);
				leaderService.resetLatch();
				
				
				/* *
				 * ���е�Follower��������Leader�������ļ��ɹ�֮�󣬽������²���:
				 * -1- ��newCore��UpdateHandler�л���REALTIMEģʽ
				 * -2- ����newCore��getSearcher()����(IndexReader.reopen())
				 * -3- ��ͣһ��дCommitLog���̺߳Ͷ�CommitLog����������Index-Builder�߳�
				 * -4- ͨ������ʱ��㣬��λ��fullAt�� ������fullAt֮ǰ�����е�Segmentɾ����
				 * -5- CommitLog��Reader��λ��fullAt��
				 * -6-  �ָ�CommitLog��д�̺߳�Index-Builder���̣߳�Index-Builder�̴߳�fullAt�㿪ʼ��ȡCommitLog���ظ�ȫ���ڼ��ʵʱ����
				 * -7- ��newCoreע�ᵽCoreContainer����ʱ����Search������˵�����е�Search���������newCore�ĵ�����
				 * -8- �ر�oldCore����������Full-Dump����ȫ������
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
				
				//��������һЩSegment�ļ�������������Reader��ָ�뵽ȫ���ָ���
				log.warn(">>>>> Full-Dump <<<<<  Clear & Reset Segment Files.");
				commitLogAccessor.clearAndReset(p);
				
				//BuildIndexJob��Ŀ������ָ���µ�UpdateHandler
				buildIndexJob.setUpdateHandler(newUpdateHandler);
				
				//��newCoreע�ᵽCoreContainer��ע��֮���µ�Search�ͻ���������µ�SolrCore��
				SolrCore oldCore = coreContainer.register(newCore, true);
				coreContainer.persist();
				
				
				try {
					/*****��ʱһ���������,ǿ�����ƿ����ü�������ֱ�ӹرյ���Ӧ��IndexReader*****/ //FIXME
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
				
				//�����رյ��ϵ�SolrCore����
				log.warn(">>>>> Full-Dump <<<<< Clean old DataDir... oldCore����·��--->>>" + oldCore.getDataDir());
				FileUtils.cleanDirectory(new File(oldCore.getDataDir()));
				
			}
			finally  {
				rDataDir = null;
				newDataDir = null;
				Thread.sleep(3000);
				buildIndexJob.resume();
			}
		} else {
			log.error("��ȡoldCore��������ַʧ�ܣ��˴�ȫ��dumpʧ��");
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
