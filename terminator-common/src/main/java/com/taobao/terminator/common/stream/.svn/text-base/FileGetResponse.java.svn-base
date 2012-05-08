package com.taobao.terminator.common.stream;

import java.io.Serializable;

public class FileGetResponse implements Serializable {
	private static final long serialVersionUID = 1659196824016862319L;
	
	public static int SUCCESS             = 0;
	public static int FILE_NOT_EXIST      = 1;
	public static int FILE_TYPE_NOT_EXIST = 2;
	public static int FILE_EXCEPTION      = 3;

	private String type   = null;
	private String name   = null;
	private long   length = 0;
	private int    code   = 0;

	public FileGetResponse(String type, String name) {
		this.type = type;
		this.name = name;
	}

	public int getCode() {
		return code;
	}

	public long getLength() {
		return length;
	}

	public String getName() {
		return name;
	}


	public String getType() {
		return type;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(String type) {
		this.type = type;
	}
}
