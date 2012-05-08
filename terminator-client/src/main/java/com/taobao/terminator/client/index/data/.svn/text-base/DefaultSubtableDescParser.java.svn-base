package com.taobao.terminator.client.index.data;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 默认的分表的描述规则的解析类 
 * 支持形如 0-7 之类的区间描述
 * 
 * @author yusen
 */
public class DefaultSubtableDescParser implements SubtableDescParser{
	private static DecimalFormat format = new DecimalFormat("_0000");

	public List<String> parse(String subtableDesc) throws SubtableDescParseException{
		String[] ss = subtableDesc.split("-");
		List<String> list = new ArrayList<String>(ss.length);
		String startStr = ss[0];
		String endStr   = ss[1];
		Integer start = Integer.valueOf(startStr);
		Integer end   = Integer.valueOf(endStr);
		
		if(start > end){
			int tmp = 0;
			tmp = start;
			start = end;
			end = tmp;
		}
		for(int i=start; i<=end ;i++){
			list.add(format.format(Integer.valueOf(i)));
		}
		return list;
	}
}
