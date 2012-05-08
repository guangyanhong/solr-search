package com.taobao.terminator.core.realtime;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

/**
 * TerminatorUpdateHandler
 * 以当前的模式为准，决定索引更新策略，目前分为direct模式和realtime模式：<br>
 * <li>
 * direct模式使用Solr自带的DirectUpdateHandler2，所有的对索引的操作直接作用于磁盘的MainIndex索引； <br> <br>
 *   
 * <li>
 * realtime模式，使用RealTimeUpdateHandler，支持实时的索引更新，实时的索引操作请求作用于内存索引，以达到实时的目的；
 * 
 * @author yusen
 */
public class TerminatorUpdateHandler extends UpdateHandler {
	protected static Log log = LogFactory.getLog(TerminatorUpdateHandler.class);
	
	public static final int MODE_DIRECT = 0;
	public static final int MODE_REALTIME =  1;
	
	protected DirectUpdateHandler2 directUpdateHandler;
	protected RealTimeUpdateHandler realTimeUpdateHandler;
	
	protected int currentMode = MODE_REALTIME;
	
	protected final Lock switchMode,accessMode;
	
	public TerminatorUpdateHandler(SolrCore solrCore) throws IOException{
		super(solrCore);
		this.directUpdateHandler = new DirectUpdateHandler2(solrCore);
		this.realTimeUpdateHandler = new RealTimeUpdateHandler(solrCore);
		
		ReadWriteLock rwLock = new ReentrantReadWriteLock();
		this.switchMode = rwLock.writeLock();
		this.accessMode = rwLock.readLock();
		
		this.initMode();
	}
	
	private void initMode(){
		String initMode = core.getSolrConfig().get("updateHandler/initMode","DIRECT");
		
		log.warn("TerminatorUpdateHandler的启动初始化模式 ==> " + initMode);
		if(initMode.equalsIgnoreCase("DIRECT")){
			this.currentMode = MODE_DIRECT;
		} else if(initMode.equalsIgnoreCase("REAL_TIME")){
			this.currentMode = MODE_REALTIME;
		} else {
			throw new RuntimeException("UNSUPPORTED MODE  { " + initMode+" } ");
		}
	}
	
	public UpdateHandler getProperUpdateHandler(){
		accessMode.lock();
		try{
			switch (currentMode) {
			case MODE_DIRECT: return directUpdateHandler;
			case MODE_REALTIME: return realTimeUpdateHandler;
			default: return directUpdateHandler;
			}
		} finally {
			accessMode.unlock();
		}
	}
	
	public void switchMode(int newMode) {
		switchMode.lock();
		try {
			this.currentMode = (newMode == MODE_DIRECT || newMode == MODE_REALTIME) ? newMode : this.currentMode;
		} finally {
			switchMode.unlock();
		}
	}

	@Override
	public int addDoc(AddUpdateCommand cmd) throws IOException {
		return this.getProperUpdateHandler().addDoc(cmd);
	}

	@Override
	public void close() throws IOException {
		this.directUpdateHandler.close();
		this.realTimeUpdateHandler.close();
	}

	@Override
	public void commit(CommitUpdateCommand cmd) throws IOException {
		this.getProperUpdateHandler().commit(cmd);
	}

	@Override
	public void delete(DeleteUpdateCommand cmd) throws IOException {
		this.getProperUpdateHandler().delete(cmd);
	}

	@Override
	public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
		this.getProperUpdateHandler().deleteByQuery(cmd);
	}

	@Override
	public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
		return this.getProperUpdateHandler().mergeIndexes(cmd);
	}

	@Override
	public void rollback(RollbackUpdateCommand cmd) throws IOException {
		this.getProperUpdateHandler().rollback(cmd);
	}

	@Override
	public Category getCategory() {
		return this.getProperUpdateHandler().getCategory();
	}

	@Override
	public String getDescription() {
		return this.getProperUpdateHandler().getDescription();
	}

	@Override
	public URL[] getDocs() {
		return this.getProperUpdateHandler().getDocs();
	}

	@Override
	public String getName() {
		return this.getProperUpdateHandler().getName();
	}

	@Override
	public String getSource() {
		return this.getProperUpdateHandler().getSource();
	}

	@Override
	public String getSourceId() {
		return this.getProperUpdateHandler().getSourceId();
	}

	@SuppressWarnings("unchecked")
	@Override
	public NamedList getStatistics() {
		return this.getProperUpdateHandler().getStatistics();
	}

	@Override
	public String getVersion() {
		return this.getProperUpdateHandler().getVersion();
	}

	public DirectUpdateHandler2 getDirectUpdateHandler() {
		return directUpdateHandler;
	}

	public void setDirectUpdateHandler(DirectUpdateHandler2 directUpdateHandler) {
		this.directUpdateHandler = directUpdateHandler;
	}

	public RealTimeUpdateHandler getRealTimeUpdateHandler() {
		return realTimeUpdateHandler;
	}

	public void setRealTimeUpdateHandler(RealTimeUpdateHandler realTimeUpdateHandler) {
		this.realTimeUpdateHandler = realTimeUpdateHandler;
	}
}
