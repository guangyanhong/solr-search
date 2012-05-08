package org.taobao.terminator.client.search4zhaopin;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.taobao.terminator.client.index.data.procesor.BoostDataProcessor;
import com.taobao.terminator.client.index.data.procesor.DataProcessor;
import org.apache.commons.lang.StringUtils;

public class JobDataProcessor implements DataProcessor {

	protected static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	protected static final DateFormat DF_D = new SimpleDateFormat("yyyyMMdd");

	@Override
	public ResultCode process(Map<String, String> map) {
//		map.put("catalogs", CommonUtil.getCatalogName(map.get("catalogs")));
		map.put("scope", CommonUtil.getBusiScopeName(map.get("scope")));
		map.put("gmtModified", getDate(map.get("gmtModified")));
		if (StringUtils.isEmpty(map.get("pubDate"))) {
			map.put("pubDate", getDate(map.get("pubDate")));
		}
		return ResultCode.SUC;
	}

	private String getDate(String date) {
		Date create = null;

		try {
			create = DF.parse(date);
		} catch (ParseException e) {
			return null;
		}

		return DF_D.format(create);
	}

	@Override
	public String getDesc() {
		return "JobDataProcessor";
	}
}

 class CommonUtil {
	 
	public static String getCatalogName(String catalog) {
		String index[] = catalog.split("-");
		StringBuffer catalogNames = new StringBuffer();
		for (String i : index) {
			catalogNames.append(
					CatalogTypeEnum.valueOf(Integer.valueOf(i)).getMeaning())
					.append(" ");
		}
		return catalogNames.toString();

	}

	public static String getBusiScopeName(String scope) {
		String index[] = scope.split("-");
		StringBuffer busiScopeNames = new StringBuffer();
		for (String i : index) {
			busiScopeNames.append(
					BusiScopeTypeEnum.valueOf(Integer.valueOf(i)).getMeaning())
					.append(" ");
		}
		return busiScopeNames.toString();

	}
	public static String getStr(String str,int length){
		if(StringUtils.isNotBlank(str)){
			str=str.replaceAll("\n", " ");
			if(str.length()>length){
				str=str.substring(0, length-3);
				str+="...";
			}
		}
		return str;
	}
}

