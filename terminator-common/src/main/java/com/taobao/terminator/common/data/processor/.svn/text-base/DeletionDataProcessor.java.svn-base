package com.taobao.terminator.common.data.processor;

import java.util.Map;

public abstract class DeletionDataProcessor implements DataProcessor{
	public static final String DELETION_KEY = "$!deleteId!$";
	
	@Override
	public String getDesc() {
		return "标记某条记录为删除状态的.";
	}

	@Override
	public ResultCode process(Map<String, String> map) throws DataProcessException {
		if(this.isDelete(map)){
			map.put(DELETION_KEY, map.get(this.getUniqueKey()));
		}
		return ResultCode.SUC;
	}
	
	protected abstract boolean isDelete(Map<String,String> map);
	protected abstract String getUniqueKey();
}
