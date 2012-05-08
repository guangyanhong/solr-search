package com.taobao.terminator.core;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.Test;

import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.core.util.IndexFileUtils;

public class IndexFileUtilTest {
	@Test
	public void test(){
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getDefault());
		
		int year = calendar.get(Calendar.YEAR);
		String month = IndexFileUtils.monthMap.get(calendar.get(Calendar.MONTH));
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		
		StringBuilder sb = new StringBuilder();
		File first = new File(sb.append(year).append("-").append(month).append("-").append(day).toString());
		File second = new File(first, String.valueOf(hour));
		File third = new File(second, String.valueOf(minute));
		
		if(!first.exists()){
			first.mkdir();
		}
		
		if(!second.exists()){
			second.mkdir();
		}
		
		if(!third.exists()){
			third.mkdir();
		}
		
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		
		String result = new StringBuilder().append(sb.toString()).append(File.separator).append(String.valueOf(hour)) 
		.append(File.separator).append(String.valueOf(minute)).append(File.separator) 
		.append(formater.format(calendar.getTime())).append(".tmp").toString();
		
		System.out.println(result);
	}
	
	@Test
	public void test2() throws ParseException{
		SimpleDateFormat formater = new SimpleDateFormat(IndexEnum.DATE_TIME_PATTERN.getValue());
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(formater.parse("2010-06-21-14-07-11-0279"));
		
		int year = calendar.get(Calendar.YEAR);
		String month = IndexFileUtils.monthMap.get(calendar.get(Calendar.MONTH));
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		
		StringBuilder sb = new StringBuilder();
		File first = new File(sb.append(year).append("-").append(month).append("-").append(day).toString());
		File second = new File(first, String.valueOf(hour));
		File third = new File(second, String.valueOf(minute));
		
		if(!first.exists()){
			first.mkdir();
		}
		
		if(!second.exists()){
			second.mkdir();
		}
		
		if(!third.exists()){
			third.mkdir();
		}
		
		String result = new StringBuilder().append(sb.toString()).append(File.separator).append(String.valueOf(hour)) 
		.append(File.separator).append(String.valueOf(minute)).append(File.separator) 
		.append(formater.format(calendar.getTime())).append(".tmp").toString();
		
		System.out.println(result);
	}
	
	@Test
	public void test3(){
		IndexFileUtils.cleanUpOldIncrXmlFile(new File("D:\\incr_xml_source"));
	}
}
