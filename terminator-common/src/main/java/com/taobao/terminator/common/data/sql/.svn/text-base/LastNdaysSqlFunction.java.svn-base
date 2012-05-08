package com.taobao.terminator.common.data.sql;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LastNdaysSqlFunction implements SqlFunction{
	private int n = 30;
	private String format = "yyyy-MM-dd 00:00:00";
	private SimpleDateFormat formater;
	
	public void init(){
		formater = new SimpleDateFormat(format);
	}
	
	@Override
	public String getPlaceHolderName() {
		return "last" + n + "Days";
	}

	@Override
	public String getValue() {
		Date d= new Date();  
        long l=d.getTime()-this.n*24*60*60*1000l;  
        d=new Date(l);
		return formater.format(d);
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}
}
