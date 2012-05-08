package org.taobao.terminator.client.search4tag;

import java.util.Map;

import com.taobao.terminator.client.router.AbstractGroupRouter;

public class Search4TagRouter extends AbstractGroupRouter{

	@Override
	public String getGroupName(Map<String, String> rowData) {
		String idStr = rowData.get(shardKey);
		long id = Long.valueOf(idStr);
		long groupNum = id % serviceConfig.getGroupNum();
		return String.valueOf(groupNum);
	}
}
