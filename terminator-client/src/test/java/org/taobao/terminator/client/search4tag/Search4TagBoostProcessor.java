package org.taobao.terminator.client.search4tag;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.terminator.client.index.data.procesor.BoostDataProcessor;

public class Search4TagBoostProcessor extends BoostDataProcessor{
	protected static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	protected static final DateFormat DF_D = new SimpleDateFormat("yyyyMMdd");

	@Override
	public ResultCode process(Map<String, String> map){
		String createTime = map.get("product_create");
		if(StringUtils.isBlank(createTime)){
			return new ResultCode("product_create�ֶ�Ϊ��");
		}
		
		Date create = null;
		try {
			create = DF.parse(createTime);
		} catch (ParseException e) {
			return new ResultCode("product_create������ȷ�����ڸ�ʽ");
		}
		
		map.put("product_create",DF_D.format(create));
		this.setBoost(map, create.getTime() * 10);
		return ResultCode.SUC;
	}
}
