package com.taobao.terminator.web.perftest.dump;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.taobao.terminator.common.data.DataProvider;

/**
 * 按照 卖家-> 买家数量分布比例生成数据
 * 
 * @author yusen
 *
 */
public class EcrmFullDataProvider3 implements DataProvider{
	
	@Override
	public boolean hasNext() throws Exception {
		return uniqueId <= 10000000;
	}
	
	@Override
	public void init() throws Exception {
	}

	@Override
	public Map<String, String> next() throws Exception {
		Map<String,String> data = new HashMap<String,String>();
		data.put("s_id", (new Random().nextInt(maxSid) + 1) + "");
		data.put("id", (uniqueId++) + "");
		data.put("count", (count++ % Short.MAX_VALUE) + "");
		data.put("am", new Random().nextInt(1000) + "");
		return data;
	}

	@Override
	public void close() throws Exception {
		count = 0;
		uniqueId = 0;
	}
	
	private long uniqueId = 0;
	private int count=0;
	private int am = 0;
	private int maxSid = 5;
}
