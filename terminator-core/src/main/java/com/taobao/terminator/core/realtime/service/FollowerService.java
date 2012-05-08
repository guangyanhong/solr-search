package com.taobao.terminator.core.realtime.service;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.core.realtime.commitlog2.SegmentPoint;

/**
 * Follower ��ɫ�����÷�������Leaderȫ�����֮��֪ͨ����ȫ��������������
 * 
 * @author yusen
 */
public interface FollowerService {
	
	/**
	 * ֪ͨFollower�����������������ļ����߳�
	 * 
	 * @param ip        Leader��IP
	 * @param port      Leader��ͬ��Index�ļ��Ķ˿�
	 * @param fileNames ��Ҫͬ���������ļ�������
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
