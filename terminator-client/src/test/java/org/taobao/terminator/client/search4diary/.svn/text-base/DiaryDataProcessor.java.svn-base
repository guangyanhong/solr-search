package org.taobao.terminator.client.search4diary;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.terminator.client.index.data.procesor.DataProcessException;
import com.taobao.terminator.client.index.data.procesor.DeletionDataProcessor;

public class DiaryDataProcessor extends DeletionDataProcessor{
	protected static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	protected static final DateFormat DF_D = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	public String getDesc() {
		return "CommentDataProcessor";
	}


	@Override
	public ResultCode process(Map<String, String> map)throws DataProcessException {
		try{
			return this.doProcess(map);
		}catch(Exception e){
			throw new DataProcessException(e);
		}
	}
	
	private ResultCode doProcess(Map<String, String> map){
		boolean isDeleted = this.isDelete(map);
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
		map.remove("isDeleted");
		map.remove("auditStatus");
		return ResultCode.SUC;
	}


	@Override
	protected String getUniqueKey() {
		return "id";
	}


	@Override
	protected boolean isDelete(Map<String, String> map) {
		String auditStatus = map.get("auditStatus");
		String isDeleted   = map.get("isDeleted"); 
		return "-1".equals(auditStatus == null ? "0" : auditStatus) || "y".equals(isDeleted == null ? "n":isDeleted);
	}}
