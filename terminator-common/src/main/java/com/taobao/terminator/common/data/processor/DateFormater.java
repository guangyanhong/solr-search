package com.taobao.terminator.common.data.processor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * 日期格式转化类
 * 
 * @author yusen
 */
public class DateFormater implements DataProcessor{
	public static final String FROM_TO_SEPERATOR = "->";
	private Map<String,String> formats;
	private Map<String,DateFormatPair> dateFormats;
	private String defaultDate;
	
	/**
	 * 使用前一定要调用该初始化方法，请配置为Spring的init-method属性
	 */
	public void init(){
		Set<String> columnNames = formats.keySet();
		if(dateFormats == null){
			dateFormats = new HashMap<String,DateFormatPair>(formats.size());
		}
		
		for(String columnName : columnNames){
			String formatStr = formats.get(columnName);
			String[] fromToStr = formatStr.split(FROM_TO_SEPERATOR);
			String from = fromToStr[0].trim();
			String to   = fromToStr[1].trim();
			dateFormats.put(columnName, new DateFormatPair(from,to));
		}
	}

	@Override
	public String getDesc() {
		return "DateFormaters";
	}
	
	public static void main(String[] args) throws InterruptedException {
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd hh:mm:ss.0");
		for(int i = 0;i < 100;i++){
			Thread.sleep(10);
			System.out.println(f.format(new Date()));
		}
	}

	@Override
	public ResultCode process(Map<String, String> map) throws DataProcessException {
		Set<String> columnNames = dateFormats.keySet();
		for(String columName : columnNames){
			DateFormatPair pair = dateFormats.get(columName);
			try {
				String value = map.get(columName);
				if(StringUtils.isBlank(value)){
					continue;
				}
				
				Date srcDate = pair.fromDateFormat.parse(value);
				String destDate = pair.toDateFormat.format(srcDate);
				map.put(columName, destDate);
			} catch (ParseException e) {
				map.put(columName, defaultDate);
			}
		}
		return ResultCode.SUC;
	}
	
	protected class DateFormatPair{
		
		public DateFormatPair(DateFormat fromDateFormat,DateFormat toDateFormat){
			this.fromDateFormat = fromDateFormat;
			this.toDateFormat   = toDateFormat;
		}
		
		public DateFormatPair(String fromDateStr,String toDateStr){
			this.fromDateFormat = new SimpleDateFormat(fromDateStr);
			this.toDateFormat   = new SimpleDateFormat(toDateStr);
		}
		
		public DateFormat fromDateFormat;
		public DateFormat toDateFormat;
	}

	public Map<String, String> getFormats() {
		return formats;
	}

	public void setFormats(Map<String, String> formats) {
		this.formats = formats;
	}

	public Map<String, DateFormatPair> getDateFormats() {
		return dateFormats;
	}

	public void setDateFormats(Map<String, DateFormatPair> dateFormats) {
		this.dateFormats = dateFormats;
	}

	public String getDefaultDate() {
		return defaultDate;
	}

	public void setDefaultDate(String defaultDate) {
		this.defaultDate = defaultDate;
	}
}
