package com.taobao.terminator.client.router;

import com.taobao.terminator.common.config.ServiceConfig;

/**
 * һ��������û�ֱ�Ӽ̳д��༴��
 * 
 * @author yusen
 */
public abstract class AbstractGroupRouter implements GroupRouter,ServiceConfigAware {
	protected ServiceConfig serviceConfig;
	protected String shardKey;

	public AbstractGroupRouter(){}
	
	public ServiceConfig getServiceConfig() {
		return serviceConfig;
	}

	@Override
	public void setServiceConfig(ServiceConfig serviceConfig) {
		this.serviceConfig = serviceConfig;
	}

	public String getShardKey() {
		return shardKey;
	}

	public void setShardKey(String shardKey) {
		this.shardKey = shardKey;
	}
}
