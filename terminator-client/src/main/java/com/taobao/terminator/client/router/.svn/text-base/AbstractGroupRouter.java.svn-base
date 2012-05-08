package com.taobao.terminator.client.router;

import com.taobao.terminator.common.config.ServiceConfig;

/**
 * 一般情况下用户直接继承此类即可
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
