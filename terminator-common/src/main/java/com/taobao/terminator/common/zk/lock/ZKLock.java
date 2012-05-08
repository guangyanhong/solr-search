package com.taobao.terminator.common.zk.lock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.zk.TerminatorZKUtils;
import com.taobao.terminator.common.zk.TerminatorZkClient;

public class ZKLock implements Lock {
	
	public static Log logger = LogFactory.getLog(ZKLock.class);
	
	private String appId;
	private String taskName;
	private TerminatorZkClient client;
	private String zkPath;
	
	public ZKLock(String appId, String taskName, TerminatorZkClient client) {
		this.appId    = appId;
		this.taskName = taskName;
		this.client   = client;
		this.init();
	}
	
	private void init(){
    	if (client == null || !client.useAble() || StringUtils.isBlank(taskName))
    		return;
    	
		String path = TerminatorZKUtils.contactZnodePaths(appId, taskName);
		path = TerminatorZKUtils.contactZnodePaths(TerminatorZKUtils.MUTEXLOCK_ROOT, path);
		path = TerminatorZKUtils.normalizePath(path);
		this.zkPath = path;
		
		try {
			if (!client.exists(path)) client.rcreate(path, null);
		} catch (Exception e) {
			logger.error("ZKLock创建BasePath失败,path ==> " + path,e);
			throw new RuntimeException("ZKLock 初始化失败.", e);
		}
	}
    
	public boolean isOwner() {
		if (client == null || !client.useAble()) 
			return false;
		String path = genLockPath(appId, taskName);
		String ip = TerminatorCommonUtils.getLocalHostIP();
		try {
			if (client.exists(path)) {
				String owner = new String(client.getData(path));
				if (ip.equals(owner)) 
					return true;
			}
		} catch (Exception e) {
			logger.debug("- ignore this exception " + e);
		}
		return false;
	}
	
	public boolean tryLock() {
		if (client == null || !client.useAble()) return false;
		String path = genLockPath(appId, taskName);
		String ip = TerminatorCommonUtils.getLocalHostIP();
		try {
			if (client.exists(path)) {
				String owner = new String(client.getData(path));
				if (ip.equals(owner)) 
					return true;
			} else {
				client.createPathIfAbsent(path, TerminatorZKUtils.toBytes(ip), false);
				if (client.exists(path)) {
					String owner = new String(client.getData(path));
					if (ip.equals(owner)) 
						return true;
				}
			}
		} catch (Exception e) {
			logger.debug("- ignore this exception " + e);
		}
		return false;
	}
	
	public String getPath() {
		return zkPath;
	}

	public boolean unlock() {
		if (client == null || !client.useAble()) return false;
		String path = genLockPath(appId, taskName);
		String ip = TerminatorCommonUtils.getLocalHostIP();
		try {
			if (client.exists(path)) {
				String owner = new String(client.getData(path));
				if (ip.equals(owner)) {
					client.delete(path);
				}
			}
		} catch (Exception e) {
			logger.debug("- ignore this exception " + e);
		}
		return true;
	}
	
	public static String genLockPath(String appId, String taskName) {
		String path = TerminatorZKUtils.contactZnodePaths(appId, taskName);
		path = TerminatorZKUtils.contactZnodePaths(TerminatorZKUtils.MUTEXLOCK_ROOT, path);
		path = TerminatorZKUtils.contactZnodePaths(path, TerminatorZKUtils.LOCK_OWNER);
		path = TerminatorZKUtils.normalizePath(path);
		return path;
	}
}
