package com.taobao.terminator.core.realtime.service;

import com.taobao.terminator.common.protocol.Address;

/**
 * Leader角色发布的服务
 * 
 * @author yusen
 *
 */
public interface LeaderService {
	
	/**
	 * 获取CommitLogSyncServer的ip:port
	 * 
	 * @return
	 */
	public Address getCLSyncAdd();
	
	/**
	 * 获取IndexSyncServer的ip:port
	 * 
	 * @return
	 */
	public Address getIndexSyncAdd();
	
	/**
	 * 全量完毕之后Follower的机器复制Leader机器的全量之后的索引文件，复制OK后Follower要向Leader报告复制状况
	 * 
	 * @param ip   Follower的机器IP
	 * @param msg  Follower想传递给Leader的消息
	 * @param isOK Follower的同步复制工作是否正常
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
