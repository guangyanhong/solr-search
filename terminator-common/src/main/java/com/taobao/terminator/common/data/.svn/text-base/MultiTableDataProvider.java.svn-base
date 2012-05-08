package com.taobao.terminator.common.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.taobao.terminator.common.data.sql.SqlFunction;

/**
 * 针对Taobao的分库分表的业务场景设计的<br>
 * 将分库分表的规则用字符串的方式描述出来，会自动进行规则的解析，将分库分表的DataProvider转换成多个SingleDataProvider<br>
 * 支持默认的分库分表表达式，可解析如：ds1:1,2,3,5-7;ds2:4(库ds1含有1，2，3，5，6，7表，库ds2 含有4表)<br>
 * 针对一些不是这样简单的分库分表的规则，可以通过自行实现TableDescriptionParser，并注入自己的分库分表描述串的方式实现<br>
 * 
 * @author yusen
 *
 */
public class MultiTableDataProvider extends MultiDataProvider implements ApplicationContextAware, PlusSqlFunctionRegisterable {
	protected static final String tableSuffixPlaceHolder = "$tablename$";
	
	protected String                 subtableDesc;
	protected TableDescriptionParser parser;
	protected String 				 sql;
	protected List<SqlFunction>      plusSqlFuncs ;
	protected ApplicationContext 	 applicationContext;
	protected JDBCProperties 		 jdbcProperties;
	
	@Override
	protected void doInit() throws Exception {
		logger.warn("初始化MultiTableDatProvider,baseSql ==> " + sql);
		if(parser == null) {
			parser = new DefaultTableDescriptionParser();
		}
		
		if(plusSqlFuncs == null) {
			plusSqlFuncs = new ArrayList<SqlFunction>();
		}
		
		if(dataProviders == null) {
			Map<String, List<String>> subtableStore = null;
			try {
				subtableStore = parser.parse(subtableDesc);
			} catch(SubtableDescParseException sdpe) {
				logger.error("解析分表规则失败", sdpe);
				throw new Exception("解析分表规则失败.",sdpe);
			}
			
			int size = 0;
			for(List<String> sublist: subtableStore.values()) {
				size += sublist.size();
			}
			
			dataProviders = new ArrayList<DataProvider>(size);
			
			int i = 0;
			for(Entry<String, List<String>> entry: subtableStore.entrySet()) {
				for(String subtable: entry.getValue()) {
					String fullSql = sql.replace(tableSuffixPlaceHolder, subtable);
					logger.warn("[" + (++i)+"] 创建TableDataProvider,替换分表名后缀 [" + tableSuffixPlaceHolder + "] 后的 sql ==> " + fullSql);
					DataSource tempDataSource = (DataSource)this.applicationContext.getBean(entry.getKey());
					
					SingleTableDataProvider dataProvider =  new SingleTableDataProvider();
					dataProvider.setDataSource(tempDataSource);
					dataProvider.setFunctionList(plusSqlFuncs);
					dataProvider.setSql(fullSql);
					dataProvider.setJdbcProperties(jdbcProperties);
					
					dataProviders.add(dataProvider);
				}
			}
		}
		currentDataProviderIndex = 0;
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}



	public String getSubtableDesc() {
		return subtableDesc;
	}

	public void setSubtableDesc(String subtableDesc) {
		this.subtableDesc = subtableDesc;
	}

	public TableDescriptionParser getParser() {
		return parser;
	}

	public void setParser(TableDescriptionParser parser) {
		this.parser = parser;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<SqlFunction> getPlusSqlFuncs() {
		return plusSqlFuncs;
	}

	public void setPlusSqlFuncs(List<SqlFunction> plusSqlFuncs) {
		this.plusSqlFuncs = plusSqlFuncs;
	}

	public JDBCProperties getJdbcProperties() {
		return jdbcProperties;
	}

	public void setJdbcProperties(JDBCProperties jdbcProperties) {
		this.jdbcProperties = jdbcProperties;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public void unregisterAll() {
		if(this.plusSqlFuncs == null) return;
		this.plusSqlFuncs.clear();
	}
	
	@Override
	public void registerSqlFunction(SqlFunction sqlFunction) {
		if(plusSqlFuncs == null){
			this.plusSqlFuncs = new ArrayList<SqlFunction>(); 
		}
		this.plusSqlFuncs.add(sqlFunction);
	}

	@Override
	public void unregisterSqlFunction(String name) {
		
	}
}
