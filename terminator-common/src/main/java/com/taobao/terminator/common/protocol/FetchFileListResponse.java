package com.taobao.terminator.common.protocol;

import java.io.Serializable;
import java.util.List;

import com.taobao.terminator.common.TerminatorCommonUtils;

public class FetchFileListResponse implements Serializable{

	private static final long serialVersionUID = -7796004384519073966L;
	
	private String masterIp = TerminatorCommonUtils.getLocalHostIP();
	private int port;
	private List<String> fileNameList = null;
	
	public FetchFileListResponse(String masterIp, int port,List<String> fileNameList) {
		this.masterIp = masterIp;
		this.fileNameList = fileNameList;
		this.port = port;
	}
	
	public String getMasterIp() {
		return masterIp;
	}
	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}
	public List<String> getFileNameList() {
		return fileNameList;
	}
	public void setFileNameList(List<String> fileNameList) {
		this.fileNameList = fileNameList;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
