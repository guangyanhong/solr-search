package com.taobao.terminator.common.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GroupConfig extends HashMap<String,HostConfig> implements Serializable{
	private static final long serialVersionUID = -4401590902703282785L;
	protected static Log log = LogFactory.getLog(GroupConfig.class);
	protected String groupName = "0";

	public GroupConfig(String groupName){
		this.groupName = groupName;
	}
	
	public HostConfig addHostConfig(HostConfig hostConfig){
		return this.put(hostConfig.getIp(), hostConfig);
	}
	
	public HostConfig getHostConfig(String ip){
		return this.get(ip);
	}
	
	public String getGroupName(){
		return this.groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		Set<String> ipSet = this.keySet();
		sb.append("   ").append(groupName).append("\n");
		for(String ip : ipSet){
			HostConfig hostConfig = this.getHostConfig(ip);
			sb.append("  ").append(hostConfig.toString()).append("\n");
		}
		return sb.toString();
	}
	
	
}
