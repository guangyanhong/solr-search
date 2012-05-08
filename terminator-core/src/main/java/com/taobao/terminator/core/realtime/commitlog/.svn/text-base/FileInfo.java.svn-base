package com.taobao.terminator.core.realtime.commitlog;

import java.io.Serializable;

public class FileInfo implements Serializable{
	private static final long serialVersionUID = -5886621089111605521L;
	
	public String fileName;
	public long size;
	
	public FileInfo(String fileName, long size) {
		super();
		this.fileName = fileName;
		this.size = size;
	}

	public String toString() {
		return "file:" + fileName + "snapshotSize:" + size;
	}
}