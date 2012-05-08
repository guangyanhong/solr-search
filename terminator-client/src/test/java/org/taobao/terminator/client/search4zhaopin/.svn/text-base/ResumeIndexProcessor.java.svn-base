/*package org.taobao.terminator.client.search4zhaopin;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.alibaba.common.lang.StringUtil;
import com.taobao.terminator.client.index.data.procesor.BoostDataProcessor;
import com.taobao.zhaopin.biz.dal.dataobject.ResumeDO;
import com.taobao.zhaopin.biz.dal.dataobject.enumType.EducationTypeEnum;
import com.taobao.zhaopin.biz.dal.dataobject.enumType.ModeTypeEnum;
import com.taobao.zhaopin.biz.dal.dataobject.enumType.SalaryTypeEnum;
import com.taobao.zhaopin.biz.dal.dataobject.enumType.SexTypeEnum;
import com.taobao.zhaopin.biz.dal.dataobject.enumType.WorkWayTypeEnum;

*//**
 * User: songjing
 * Date: 2010-7-1
 * Time: 19:39:24
 * To change this template use File | Settings | File Templates.
 *//*
public class ResumeIndexProcessor extends BoostDataProcessor{
	protected static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	protected static final DateFormat DF_D = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public ResultCode process(Map<String, String> map){
		map.put("catalogs",ResumeDO.getCatalogName(map.get("catalogs")));
		map.put("scope",ResumeDO.getBusiScopeName(map.get("scope")));
		map.put("mode", ModeTypeEnum.getNameByType(Integer.valueOf(map.get("mode"))));
		map.put("workWay", WorkWayTypeEnum.getNameByType(Integer.valueOf(map.get("workWay"))));
		map.put("eduLevel", EducationTypeEnum.getEducationType(map.get("eduLevel")).getDesc());
		map.put("sex", SexTypeEnum.getNameByType(Integer.valueOf(map.get("sex"))));
		map.put("gtmModified", getDate(map.get("gtmModified")));
	    map.put("birthday", getDate(map.get("birthday")));
		map.put("introduction", getStr(map.get("introduction")));
		return ResultCode.SUC;
	}
	
	private static String getStr(String str){
		if(StringUtil.isNotBlank(str)){
			str=str.replaceAll("\n", " ");
			if(str.length()>117){
				str=str.substring(0, 114);
				str+="...";
			}
		}
		return str;
	}
	
	private String getDate(String date){
		Date create = null;
		try {
			create = DF.parse(date);
		} catch (ParseException e) {
			return null;
		}
		
		return DF_D.format(create);
	}
	
}


*/