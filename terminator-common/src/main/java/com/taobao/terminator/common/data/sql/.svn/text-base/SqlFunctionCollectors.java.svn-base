package com.taobao.terminator.common.data.sql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SqlFunctionCollectors {
	private Map<String,SqlFunction> functions = null;
	
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
}
