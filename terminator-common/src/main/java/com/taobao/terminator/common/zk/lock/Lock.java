package com.taobao.terminator.common.zk.lock;

/**
 * most of the lock implements are object waiting blocking
 * so pay attention to thread.interrupt mark
 */
public interface Lock {
	
	/**
	 * try to get the lock, return true if success or false immediately
	 * return true if this machine already hold the lock
	 * return false if ZooKeeper service is disable 
	 */
	public boolean tryLock();
	
	/**
	 * release the lock and return true if success
	 * return false if ZooKeeper service is disable 
	 */
	public boolean unlock();
	
	/**
	 * return true if currently runnable get the lock
	 * return false if ZooKeeper service is disable 
	 */
	public boolean isOwner();
	
}
