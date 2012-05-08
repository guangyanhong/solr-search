package com.taobao.terminator.core.realtime.commitlog2;

import java.io.Serializable;

/**
 * 同步CommitLog的Response响应对象
 * 
 * @author yusen
 *
 */
public class SyncResponse implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static final int CODE_FILE_NOT_FOUND = 1;
	public static final int CODE_OFFSET_ERROR   = 2;
	public static final int CODE_OK             = 0;
	
	private int     code   = CODE_OK;
	private int     length = 0;
	private boolean isEOF  = false;
	private boolean hasNextFile = true;
	private String  fileName = null;
	
	public boolean isOk() {
		return this.code == CODE_OK;
	}
	
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public boolean isEOF() {
		return isEOF;
	}
	public void setEOF(boolean isEOF) {
		this.isEOF = isEOF;
	}
	public boolean isHasNextFile() {
		return hasNextFile;
	}
	public void setHasNextFile(boolean hasNextFile) {
		this.hasNextFile = hasNextFile;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
