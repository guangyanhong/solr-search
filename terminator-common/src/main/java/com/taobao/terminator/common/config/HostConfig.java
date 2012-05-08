package com.taobao.terminator.common.config;

import java.util.Properties;

public class HostConfig extends Properties{

	private static final long serialVersionUID = -4586146581307582203L;
	
	private String ip;
	
	public static final String DEFAULT_IS_READER = "true";
	public static final String DEFAULT_IS_MERGER = "true";
	public static final String DEFAULT_IS_INDEXWRITER = "true";
	public static final String DEFAULT_IS_WRITER = "false";
	public static final String DEFAULT_PORT      = "false";
	
	public HostConfig(){}
	
	/**
	 * @param hostInfo  ¸ñÊ½Îª 192.168.222.32:8080#1:1:1   ==> ip:port # reader:writer:merger
	 */
	public HostConfig(String hostInfo){
		String[] iprole = hostInfo.split("#");
		if(iprole.length != 2){
			throw new IllegalArgumentException("The hostInfo must be like this [192.168.222.32:8080#1:1:1]" );
		}
		String[] ippart = iprole[0].split(":");
		String[] rolepart = iprole[1].split(":");
		
		if(ippart.length !=2 || rolepart.length != 3){
			throw new IllegalArgumentException("The hostInfo must be like this [192.168.222.32:8080#1:1:1]" );
		}
		
		this.setIp(ippart[0]);
		this.setPort(ippart[1]);	
		
		this.setReader(rolepart[0].equals("1"));
		this.setWriter(rolepart[1].equals("1"));
		this.setMerger(rolepart[2].equals("1"));
	}
	
	public void setIp(String ip){
		this.ip = ip;
	}
	
	public String getIp(){
		return this.ip;
	}
	
	public boolean isWriter(){
		return Boolean.valueOf(this.getProperty("isWriter",DEFAULT_IS_WRITER));
	}
	
	public boolean isReader(){
		return Boolean.valueOf(this.getProperty("isReader",DEFAULT_IS_READER));
	}

	public boolean isMerger(){
		return Boolean.valueOf(this.getProperty("isMerger",DEFAULT_IS_MERGER));
	}
	
	public void setMerger(boolean isMerger){
		this.setProperty("isMerger", String.valueOf(isMerger));
	}
	
	public void setReader(boolean isReader){
		this.setProperty("isReader", String.valueOf(isReader));
	}
	
	public void setWriter(boolean isWriter){
		this.setProperty("isWriter", String.valueOf(isWriter));
	}
	
	public String getPort(){
		return this.getProperty("port",DEFAULT_PORT);
	}
	
	public void setPort(String port){
		this.setProperty("port", port);
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("      ").append(this.getIp()).append(":").append(this.getPort());
		if(this.isMerger()){
			sb.append(" merger ");
		}
		
		if(this.isReader()){
			sb.append(" reader ");
		}
		
		if(this.isWriter()){
			sb.append(" writer ");
		}
		
		return sb.toString();
	}
}
