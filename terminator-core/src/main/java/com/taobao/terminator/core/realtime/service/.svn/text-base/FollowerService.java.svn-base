package com.taobao.terminator.core.realtime.service;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;

/**
 * Follower 角色发布该服务，用于Leader全量完毕之后通知进行全量拷贝索引工作
 * 
 * @author yusen
 */
public interface FollowerService {
	
	/**
	 * 通知Follower机器开启拷贝索引文件的线程
	 * 
	 * @param ip        Leader的IP
	 * @param port      Leader的同步Index文件的端口
	 * @param fileNames 需要同步的索引文件的名称
	 */
	public int notifyFollower(String ip,int port,String[] fileNames,SegmentPoint fullPoint);
	
	public static class Utils {
		public static final String CS_DATAID_PREFIX = "TERMINATOR-INNDER-SERVER-GROUP";
		public static final String HSF_VERSION_PREFIX = "TERMINATOR-INNER-SERVICE";
		public static final String DEFAULT_HSF_GROUP = "TERMINATOR-GROUP";
		
		public static String genCSDataId(String coreName) {
			return new StringBuilder().append(CS_DATAID_PREFIX).append("-").append(coreName).toString();
		}
		
		public static String gentHsfVersion(String coreName,String ip) {
			return new StringBuilder().append(HSF_VERSION_PREFIX).append("-").append(coreName).append("-").append(ip).toString();
		}
		
		public static String genHsfVersion(String coreName) {
			return gentHsfVersion(coreName, TerminatorCommonUtils.getLocalHostIP());
		}
	}
}
