package com.taobao.terminator.client.router;

import com.taobao.terminator.common.config.ServiceConfig;

/**
 * @author yusen
 */
public interface ServiceConfigAware {
	public void setServiceConfig(ServiceConfig serviceConfig);
}
