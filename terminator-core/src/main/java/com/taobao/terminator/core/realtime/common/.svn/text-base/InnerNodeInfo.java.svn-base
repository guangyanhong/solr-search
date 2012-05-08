package com.taobao.terminator.core.realtime.common;

import java.io.Serializable;

import com.taobao.terminator.common.TerminatorCommonUtils;

/**
 * 内部节点的信息，用于组内机器通讯
 * 
 * @author yusen
 */
public class InnerNodeInfo implements Serializable{

	private static final long serialVersionUID = 514382438324119093L;
	
	public String ip;
	public int commitLogSyncPort;
	public int indexSyncPort;
	private String hsfVersion;
	public String coreName;
	
	public InnerNodeInfo(String ip, int commitLogSyncPort, int indexSyncPort, String coreName) {
		super();
		this.ip = ip;
		this.commitLogSyncPort = commitLogSyncPort;
		this.indexSyncPort = indexSyncPort;
		this.coreName = coreName;
		this.hsfVersion = this.generateHsfVersion();
	}

	public String toString() {
		return "InnerNodeInfo [commitLogSyncPort=" + commitLogSyncPort + ", hsfVersion=" + hsfVersion + ", indexSyncPort=" + indexSyncPort + ", ip=" + ip + "]";
	}
	
	public String generateHsfVersion() {
		if(hsfVersion == null) {
			StringBuilder sb = new StringBuilder();
			sb.append("TERMINATOR-INNSERSERVICE-").append(coreName).append("-").append(TerminatorCommonUtils.getLocalHostIP());
			this.hsfVersion = sb.toString();
		} 
		return hsfVersion;
	}
}
