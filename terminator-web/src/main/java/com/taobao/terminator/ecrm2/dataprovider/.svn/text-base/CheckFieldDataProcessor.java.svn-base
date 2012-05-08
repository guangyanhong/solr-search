package com.taobao.terminator.ecrm2.dataprovider;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.taobao.terminator.common.data.processor.DataProcessException;
import com.taobao.terminator.common.data.processor.DataProcessor;

public class CheckFieldDataProcessor implements DataProcessor{

	@Override
	public String getDesc() {
		return "Check File Value";
	}

	public static Set<String> sIds = new HashSet<String>();
	static {
		sIds.add("144939");
		sIds.add("23458207");
		sIds.add("20213105");
		sIds.add("435179831");
		sIds.add("22670458");
		sIds.add("434568395");
	}
	
	@Override
	public ResultCode process(Map<String, String> map) throws DataProcessException {
		
		String s_id = map.get("s_id");
		if(!sIds.contains(s_id)) {
			return ResultCode.FAI;
		}
		
		checkRangeShort("i_c",   map);
		checkRangeShort("count", map);
		checkRangeShort("c_n",   map);
		checkRangeInteger("am",  map);
		checkRangeInteger("l_t", map);
		checkRangeInteger("a_p", map);
		
		return ResultCode.SUC;
	}
	
	private void checkRangeShort(String fn,Map<String, String> map){
		try {
			Short.parseShort(map.get(fn));
		} catch (NumberFormatException e) {
			map.put(fn, Short.MAX_VALUE + "");
			throw new RuntimeException("超过short的值域 ==>"  + fn + ":" + map.get(fn));
		} 
	}
	
	private void checkRangeInteger(String fn,Map<String, String> map){
		try {
			Integer.parseInt(map.get(fn));
		} catch (NumberFormatException e) {
			map.put(fn, Integer.MAX_VALUE + "");
			throw new RuntimeException("超过int的值域 ==> " + fn + ":" + map.get(fn));
		} 
	}
}
