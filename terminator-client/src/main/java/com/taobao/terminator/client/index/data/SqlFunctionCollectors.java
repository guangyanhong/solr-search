package com.taobao.terminator.client.index.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.taobao.terminator.client.index.timer.TimeManageException;
import com.taobao.terminator.client.index.timer.ZKTimeManager;
import com.taobao.terminator.client.index.timer.TimerManager.StartAndEndTime;

public class SqlFunctionCollectors {
	private Map<String,SqlFunction> functions = null;
	private String serviceName = null;
	
	public SqlFunctionCollectors(String serviceName){
		this.serviceName = serviceName;
	}
	
	/**
	 * 初始化默认绑定的几个Function
	 */
	public void initDefaultFunctions(){
		this.register(new LastModifiedFuncion());
		this.register(new StartDateFunction());
		this.register(new EndDateFunction());
		this.register(new NowFunction());
	}
	
	public SqlFunction register(SqlFunction function){
		if(functions == null){
			functions = new HashMap<String,SqlFunction>();
		}
		return functions.put(function.getPlaceHolderName(), function);
	}
	
	public String parseSql(String sql){
		Iterator<String> i = SqlUtils.parseFunctions(sql);
		
		if(i == null) {
			return sql;
		}
		
		while(i.hasNext()){
			String funcName = i.next();
			SqlFunction function = functions.get(funcName);
			if(function == null){
				throw new RuntimeException("没有定义的SQL的Function  ==> " + funcName);
			}
			String placeHolderName = function.getPlaceHolderName();
			String value           = function.getValue();
			sql = sql.replace(SqlUtils.PLACE_HOLDER_CHAR + placeHolderName + SqlUtils.PLACE_HOLDER_CHAR, value);
		}
		return sql;
	}
	
	public String getValue(String name) {
		SqlFunction func = this.functions.get(name);
		if(func == null) 
			throw new NullPointerException("不包含名为 ==> " + name + "的函数.");
		
		return func.getValue();
	}
	
	public class LastModifiedFuncion implements SqlFunction{

		@Override
		public String getPlaceHolderName() {
			return "lastModified";
		}

		@Override
		public String getValue() {
			StartAndEndTime ts = null;
			Date date = null;
			try {
				ts = ZKTimeManager.getInstance(serviceName).justGetTimes();
				date  = ts.startTime;
			} catch (TimeManageException e) {
				date = new Date();
			}
			return SqlUtils.parseDate(date);
		}
	}
	
	public class StartDateFunction implements SqlFunction{

		@Override
		public String getPlaceHolderName() {
			return "startDate";
		}

		@Override
		public String getValue() {
			StartAndEndTime times = null;
			try {
				times = ZKTimeManager.getInstance(serviceName).justGetTimes();
			} catch (TimeManageException e) {
				e.printStackTrace();
			}
			return SqlUtils.parseDate(times.startTime);
		}
	}
	
	public class EndDateFunction implements SqlFunction{

		@Override
		public String getPlaceHolderName() {
			return "endDate";
		}

		@Override
		public String getValue() {
			StartAndEndTime times = null;
			try {
				times = ZKTimeManager.getInstance(serviceName).justGetTimes();
			} catch (TimeManageException e) {
				e.printStackTrace();
			}
			return SqlUtils.parseDate(times.endTime);
		}
	}
	
	public class NowFunction implements SqlFunction{

		@Override
		public String getPlaceHolderName() {
			return "now";
		}

		@Override
		public String getValue() {
			return SqlUtils.parseDate(new Date());
		}
	}
}
