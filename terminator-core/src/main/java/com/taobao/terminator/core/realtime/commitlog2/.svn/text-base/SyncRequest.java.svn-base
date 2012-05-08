package com.taobao.terminator.core.realtime.commitlog2;

import java.io.Serializable;

import com.taobao.terminator.common.TerminatorCommonUtils;

/**
 * 同步CommitLog的Request请求对象
 * 
 * @author yusen
 *
 */
public class SyncRequest implements Serializable{
	private static final long serialVersionUID = 9057873186376964998L;
	
	private boolean isSyncCheckpoint = false;
	private String segmentName;
	private int    offset;
	private boolean isFirstSync = false;
	private String ip = TerminatorCommonUtils.getLocalHostIP();
	
	public String getSegmentName() {
		return segmentName;
	}
	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	public boolean isFirstSync() {
		return isFirstSync;
	}
	public void setFirstSync(boolean isFirstSync) {
		this.isFirstSync = isFirstSync;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public boolean isSyncCheckpoint() {
		return isSyncCheckpoint;
	}
	public void setSyncCheckpoint(boolean isSyncCheckpoint) {
		this.isSyncCheckpoint = isSyncCheckpoint;
	}
}
