package com.taobao.terminator.common.data.sql;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {
	public static final Pattern PARTTERN = Pattern.compile("\\$([A-Za-z0-9]*?)\\$", Pattern.DOTALL);
	public static final String PLACE_HOLDER_CHAR = "$";
	public static DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * ��ȡSql�е�վλ����ÿ��ռλ����Ӧ��һ�������Function��ʵ��
	 * 
	 * @param sql
	 * @return
	 */
	public static Iterator<String> parseFunctions(String sql){
		Set<String> res = null;
		Matcher matcher = PARTTERN.matcher(sql);
		while (matcher.find()) {
			if (res == null) {
				res = new HashSet<String>();
			}
			res.add(matcher.group(1));
		}
		return res != null?res.iterator()  : null;
	}
	
	public static String parseDate(Date date){
		return DF.format(date);
	}

}
