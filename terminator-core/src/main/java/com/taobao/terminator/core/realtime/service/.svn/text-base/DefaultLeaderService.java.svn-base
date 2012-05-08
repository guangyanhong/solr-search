package com.taobao.terminator.core.realtime.service;

import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.protocol.Address;
import com.taobao.terminator.common.stream.FileGetServer;
import com.taobao.terminator.core.realtime.commitlog2.CommitLogSyncServer;

public class DefaultLeaderService implements LeaderService{
	private final Log log = LogFactory.getLog(DefaultLeaderService.class);
	private CommitLogSyncServer clSyncServer;
	private FileGetServer       indexSyncServer;
	
	public DefaultLeaderService(CommitLogSyncServer clSyncServer,FileGetServer indexSyncServer) {
		this.clSyncServer = clSyncServer;
		this.indexSyncServer = indexSyncServer;
	}
	
	private CountDownLatch latch = null;
	public synchronized void registerLatch(CountDownLatch latch) {
		this.latch = latch;
		this.notify();
	}
	
	public void resetLatch() {
		this.latch = null;
	}
	
	/**
	 * 如果CommigLogSyncServer没有启动或者死掉了，先start而后返回Address<br>
	 * 如果启动出现了异常则返回null
	 */
	@Override
	public synchronized Address getCLSyncAdd() {
		if(!clSyncServer.isAlive()) {
			try {
				clSyncServer.start();
			} catch (Exception e) {
				return null;
			}
		}
		return clSyncServer.getAdd();
	}

	/**
	 * 如果FileGetServer没有启动或者死掉了，先start而后返回Address<br>
	 * 如果启动出现了异常则返回null
	 */
	@Override
	public synchronized Address getIndexSyncAdd() {
		if(!indexSyncServer.isAlive()) {
			try {
				indexSyncServer.start();
			} catch (Exception e) {
				return null;
			}
		}
		return indexSyncServer.getAdd();
	}

	@Override
	public synchronized boolean report(String ip, boolean isOK, String msg) {
		if(latch == null) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				
			}
		}
		log.warn("Follower [" + ip + "] report ==> " + (isOK ? "SUC" : "FAILED") + "  msg ==> " + msg);
		latch.countDown();
		return true;
	}
}
