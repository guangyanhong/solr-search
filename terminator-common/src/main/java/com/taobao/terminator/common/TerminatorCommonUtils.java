package com.taobao.terminator.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;


public class TerminatorCommonUtils {
	private static String localIp = null;
	
	/**
	 * 获取本机IP
	 * 
	 * @return
	 */
	public static final String getLocalHostIP(){
		if(StringUtils.isBlank(localIp)){
			try {
				localIp = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				throw new RuntimeException("[local-ip] an exception occured when get local ip address", e);
			}
		}
		return localIp;
	}
	
	/**
	 * 判断coreName是否是合法的形式 serviceName-groupName 如 search4album-0
	 * 
	 * @param coreName
	 * @return
	 */
	public static final boolean isCorrectCoreName(String coreName){
		if(!StringUtils.isBlank(coreName)){
			String ss[] = coreName.split(TerminatorConstant.CORENAME_SEPERATOR);
			return ss.length == 2;
		}
		return false;
	}
	
	/**
	 * 拆分coreName为serviceName  groupName两部分
	 * 
	 * @param coreName
	 * @return
	 */
	public static String[] splitCoreName(String coreName){
		if(isCorrectCoreName(coreName)){
			return coreName.split(TerminatorConstant.CORENAME_SEPERATOR);
		}else{
			return null;
		}
	}
	
	public static String formatDate(Date date){
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
	}
	
	public static Date parseDate(String str) throws ParseException{
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str);
	}
}
