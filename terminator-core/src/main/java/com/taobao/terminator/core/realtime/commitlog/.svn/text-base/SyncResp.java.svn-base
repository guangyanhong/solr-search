package com.taobao.terminator.core.realtime.commitlog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SyncResp implements Serializable{
	private static final long serialVersionUID = -189945378810702001L;
	
	public int resultCode;
	public List<FileInfo> fileInfos;
	
	public static final int FILE_NOT_FOUND = -1;
	public static final int REQ_ERROR      = -2;

	public static Map<Integer,String> errorMsgMaps;
	
	static {
		errorMsgMaps = new HashMap<Integer,String>();
		errorMsgMaps.put(FILE_NOT_FOUND, "File-Not-Found-Error!");
		errorMsgMaps.put(REQ_ERROR, "Request-Parameter-Error!");
	}
	
	public SyncResp(int resultCode,List<FileInfo> fileInfos) {
		this.resultCode = resultCode;
		this.fileInfos = fileInfos;
	}
	
	public boolean isSuc() {
		return this.resultCode != -1;
	}
	
	public String toString() {
		return this.isSuc() ? fileInfos.toString() : (errorMsgMaps.get(resultCode) == null ? "Other-Error!" : errorMsgMaps.get(resultCode));
	}
}
