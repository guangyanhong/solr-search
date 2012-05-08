package com.taobao.terminator.common.data;

import com.taobao.terminator.common.data.sql.SqlFunction;

public interface PlusSqlFunctionRegisterable {
	public void registerSqlFunction(SqlFunction sqlFunction);
	
	public void unregisterSqlFunction(String name);
	
	public void unregisterAll();
}
