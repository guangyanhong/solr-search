package com.taobao.terminator.common.protocol;

import java.io.Serializable;

public class Address implements Serializable{
	private static final long serialVersionUID = 2786525881587080718L;
	
	private String ip;
	private int port;
	
	public Address(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return "Address [ip=" + ip + ", port=" + port + "]";
	}
}
