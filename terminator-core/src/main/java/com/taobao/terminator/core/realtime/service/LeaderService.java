package com.taobao.terminator.core.realtime.service;

import com.taobao.terminator.common.protocol.Address;

/**
 * Leader��ɫ�����ķ���
 * 
 * @author yusen
 *
 */
public interface LeaderService {
	
	/**
	 * ��ȡCommitLogSyncServer��ip:port
	 * 
	 * @return
	 */
	public Address getCLSyncAdd();
	
	/**
	 * ��ȡIndexSyncServer��ip:port
	 * 
	 * @return
	 */
	public Address getIndexSyncAdd();
	
	/**
	 * ȫ�����֮��Follower�Ļ�������Leader������ȫ��֮��������ļ�������OK��FollowerҪ��Leader���渴��״��
	 * 
	 * @param ip   Follower�Ļ���IP
	 * @param msg  Follower�봫�ݸ�Leader����Ϣ
	 * @param isOK Follower��ͬ�����ƹ����Ƿ�����
	 * @return
	 */
	public boolean report(String ip,boolean isOK,String msg);
	
	public static class Utils {
		public static String genHsfVersion(String coreName) {
			return new StringBuilder().append("LEADER-SERVICE").append("-").append(coreName).toString();
		}
		
		public static String genCSDataId(String coreName) {
			return new StringBuilder().append(LeaderService.class.getName()).append(":").append(genHsfVersion(coreName)).toString();
		}
	}
}
