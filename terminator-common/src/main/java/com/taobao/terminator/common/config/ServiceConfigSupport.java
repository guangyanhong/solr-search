package com.taobao.terminator.common.config;

public interface ServiceConfigSupport {
	public void onServiceConfigChange(ServiceConfig serviceConfig);
	public ServiceConfig getServiceConfig();
}
