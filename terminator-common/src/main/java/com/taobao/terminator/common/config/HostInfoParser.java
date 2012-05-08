package com.taobao.terminator.common.config;


public class HostInfoParser {
	
	/**
	 * 192.168.222.32:8080#1:1:1:1
	 * reader : writer : merger : indexWriter
	 * 
	 * @param hostInfo
	 * @return
	 */
	public static HostConfig toHostConfig(String hostInfo){
		HostConfig hostConfig = new HostConfig();
		String[] iprole = hostInfo.split("#");
		if(iprole.length != 2){
			throw new IllegalArgumentException("The hostInfo must be like this [192.168.222.32:8080#1:1:1]" );
		}
		String[] ippart = iprole[0].split(":");
		String[] rolepart = iprole[1].split(":");
		
		if(ippart.length !=2 || rolepart.length != 3){
			throw new IllegalArgumentException("The hostInfo must be like this [192.168.222.32:8080#1:1:1]" );
		}
		
		hostConfig.setIp(ippart[0]);
		hostConfig.setPort(ippart[1]);	
		
		hostConfig.setReader(rolepart[0].equals("1"));
		hostConfig.setWriter(rolepart[1].equals("1"));
		hostConfig.setMerger(rolepart[2].equals("1"));
		
		return hostConfig;
	}
	
	/**
	 * 192.168.222.32:8080#1:1:1:1
	 * reader : writer : merger
	 * 
	 * @param hostInfo
	 * @return
	 */
	public static String toHostInfo(HostConfig hostConfig){
		StringBuilder sb = new StringBuilder();
		
		sb.append(hostConfig.getIp());
		sb.append(":");
		sb.append(hostConfig.getPort());
		sb.append("#");
		sb.append(hostConfig.isReader()?"1":"0");
		sb.append(":");
		sb.append(hostConfig.isWriter()?"1":"0");
		sb.append(":");
		sb.append(hostConfig.isMerger()?"1":"0");
		
		return sb.toString();
	}
}
