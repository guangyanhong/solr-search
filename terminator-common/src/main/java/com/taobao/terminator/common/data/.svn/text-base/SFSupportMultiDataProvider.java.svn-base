package com.taobao.terminator.common.data;

import java.util.ArrayList;
import java.util.List;

import com.taobao.terminator.common.data.sql.SqlFunction;

/**
 * Ö§³ÖSqlFunctionµÄMultiDataProvider 
 */
public class SFSupportMultiDataProvider extends MultiDataProvider implements PlusSqlFunctionRegisterable{

	protected List<SqlFunction> functionList;
	
	@Override
	protected void doInit() throws Exception {
		for(DataProvider dp : dataProviders) {
			if(dp instanceof PlusSqlFunctionRegisterable) {
				for(SqlFunction f : this.functionList) {
					((PlusSqlFunctionRegisterable) dp).registerSqlFunction(f);
				}
			}
		}
	}

	@Override
	public void unregisterAll() {
		if(this.functionList == null) 
			return;
		this.functionList.clear();
	}
	
	@Override
	public void registerSqlFunction(SqlFunction sqlFunction) {
		if(functionList == null){
			this.functionList = new ArrayList<SqlFunction>(); 
		}
		this.functionList.add(sqlFunction);
	}

	@Override
	public void unregisterSqlFunction(String name) {
		
	}
}
