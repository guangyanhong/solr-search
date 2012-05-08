package com.taobao.terminator.core.realtime;

/**
 * 标记可自己发布自己为HSF服务的接口
 * 
 * @author yusen
 *
 */
public interface SelfPublisher {
	public void publishHsfService(String coreName);
}
