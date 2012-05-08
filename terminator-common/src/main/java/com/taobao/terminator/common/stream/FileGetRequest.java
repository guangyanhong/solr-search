package com.taobao.terminator.common.stream;

import java.io.Serializable;

public class FileGetRequest implements Serializable {

	private static final long serialVersionUID = 6273053372909908509L;

	private String type = null;
	private String name = null;

	public FileGetRequest(String type, String name) {
		this.type = type;
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(String type) {
		this.type = type;
	}
}
