package org.taobao.terminator.client.search4comment;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.terminator.client.index.data.procesor.DeletionDataProcessor;

public class CommentDataProcessor extends DeletionDataProcessor{
	protected static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	protected static final DateFormat DF_D = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	public String getDesc() {
		return "CommentDataProcessor";
	}


	@Override
	public ResultCode process(Map<String, String> map){
		
		String id = map.get("id");
		String fromType = map.get("fromType");
		map.put("id", fromType + "-" + id);
	
		boolean isDeleted = this.isDelete(map);
		if(id.equals("6") || id.equals("19") || id.equals("8")){
			isDeleted = true;
		}
		if(isDeleted){
			map.put(DELETION_KEY, map.get(this.getUniqueKey()));
			return ResultCode.SUC;
		}
	
		String createTime = map.get("gmtCreate");
		if(StringUtils.isBlank(createTime)){
			return new ResultCode("gmtCreate字段为空");
		}
		
		Date create = null;
		try {
			create = DF.parse(createTime);
		} catch (ParseException e) {
			return new ResultCode("gmtCreate不是正确的日期格式");
		}
		
		map.put("gmtCreate",DF_D.format(create));
		map.put("gmtCreateTime", String.valueOf(create.getTime()));
		map.remove("isDeleted");
		return ResultCode.SUC;
	}


	@Override
	protected String getUniqueKey() {
		return "id";
	}


	@Override
	protected boolean isDelete(Map<String, String> map) {
		return map.get("isDeleted").equals("1");
	}
}
