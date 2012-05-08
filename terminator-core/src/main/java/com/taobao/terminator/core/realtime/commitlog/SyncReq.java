package com.taobao.terminator.core.realtime.commitlog;

import java.io.Serializable;

public class SyncReq implements Serializable{
	private static final long serialVersionUID = -3957804414119910537L;
	
	public String startFileName;
	public SyncReq(String startFileName) {
		this.startFileName = startFileName;
	}
}
