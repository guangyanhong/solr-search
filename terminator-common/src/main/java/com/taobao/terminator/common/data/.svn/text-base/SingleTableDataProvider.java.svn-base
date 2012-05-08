package com.taobao.terminator.common.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.data.sql.SqlFunction;
import com.taobao.terminator.common.data.sql.SqlFunctionCollectors;

public class SingleTableDataProvider extends AbstractDataProvider implements PlusSqlFunctionRegisterable{
	
	protected static Log logger = LogFactory.getLog(SingleTableDataProvider.class);
	
	protected DataSource     	dataSource;
	protected Connection 		connection;
	protected Statement  		statement;
	protected ResultSetMetaData metaData;
	protected ResultSet  		resultSet;
	protected int 		 		columCount;
	protected JDBCProperties 	jdbcProperties;
	protected String         	sql;
	protected List<SqlFunction> functionList;
	protected SqlFunctionCollectors functionCollectors;
	//protected Map<String,String> row =  
	
	protected Map<String,String> row;
	
	
	@Override
	protected void doInit() throws Exception {
		try{
			this.connection = this.dataSource.getConnection();

			if(this.jdbcProperties == null && this.dataSource instanceof JDBCPropertiesSupport){
				this.jdbcProperties = ((JDBCPropertiesSupport)dataSource).getJdbcProperties();
			}
			
			this.statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			if(this.jdbcProperties!=null) {
				this.setPropertiesBeforeQuery(this.statement,this.jdbcProperties);
			}
			
			String executeSql = this.buildSql(sql);
			logger.warn("执行SQL ==> \n" + executeSql);
			
			if(statement.execute(executeSql)){
				resultSet = statement.getResultSet();
			}else{
				logger.fatal("执行SQL失败,sql ==> \n" + executeSql);
				resultSet = null;
			}
			metaData   = resultSet.getMetaData();
			columCount = metaData.getColumnCount();
			
			
		} catch(Exception e){
			this.close();
			throw new Exception("SingleTableDataProvider初始化异常,直接释放所有资源，调用close方法.",e);
		}
	}
	
	protected String buildSql(String sql){
		if(functionCollectors == null){
			functionCollectors = new SqlFunctionCollectors();
		}
		
		if(functionList != null && functionList.size() != 0) {
			for(SqlFunction func : functionList){
				functionCollectors.register(func);
			}
		}
		
		return functionCollectors.parseSql(sql);
	}
	
	/**
	 * 针对一些JDBC的参数，可在这里实现，用户可覆盖这个方法，想做什么都在这里做吧
	 * 
	 * @param statement
	 * @param jdbcProperties
	 * @throws SQLException
	 */
	protected void setPropertiesBeforeQuery(Statement statement,JDBCProperties jdbcProperties) throws SQLException {
		logger.warn("设置JDBC的相关属性 ==> " + jdbcProperties.toString());
		statement.setFetchSize(jdbcProperties.getStatementFetchSize());
	}

	@Override
	protected void doClose() throws Exception {
		logger.warn("关闭DataProvider,释放数据库连接资源，状态标志归位.");
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if(connection != null)
				connection.close();
		} catch (SQLException e) {
			throw new Exception("释放数据库连接资源失败",e);
		} finally {
			resultSet   = null;
			statement   = null;
			connection  = null;
			metaData    = null;
			columCount  = 0;
		}
	}

	@Override
	public boolean hasNext() throws Exception {
		return resultSet != null && resultSet.next();
	}

	
	@Override
	public Map<String, String> next() throws Exception {
		//此处设置为null，便于GC回收
		if(row != null) {
			row = null;
		}
		row = new HashMap<String,String>(columCount);
		
		for(int i = 1; i <= columCount; i++){				
			String key   = null;
			String value = null;
			try {
				key = metaData.getColumnLabel(i);
				value = resultSet.getString(i);
			} catch (SQLException e) {
				if(ignoreIllFieldRow) {
					throw new Exception("调用next方法失败",e);
				} else {
					row.put(key, defaultIllFiledValue);
					continue;
				}
			}
			row.put(key, value != null ? value : " ");
		}
		
		return row;
	}
	
	private boolean ignoreIllFieldRow = true;
	private String  defaultIllFiledValue = null;
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Connection getConnection() {
		return connection;
	}
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public Statement getStatement() {
		return statement;
	}
	public void setStatement(Statement statement) {
		this.statement = statement;
	}
	public ResultSet getResultSet() {
		return resultSet;
	}
	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	public ResultSetMetaData getMetaData() {
		return metaData;
	}
	public void setMetaData(ResultSetMetaData metaData) {
		this.metaData = metaData;
	}
	public int getColumCount() {
		return columCount;
	}
	public void setColumCount(int columCount) {
		this.columCount = columCount;
	}
	public DataSource getDataSource() {
		return dataSource;
	}
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	public JDBCProperties getJdbcProperties() {
		return jdbcProperties;
	}
	public void setJdbcProperties(JDBCProperties jdbcProperties) {
		this.jdbcProperties = jdbcProperties;
	}
	public List<SqlFunction> getFunctionList() {
		return functionList;
	}
	public void setFunctionList(List<SqlFunction> functionList) {
		this.functionList = functionList;
	}
	public SqlFunctionCollectors getFunctionCollectors() {
		return functionCollectors;
	}
	public void setFunctionCollectors(SqlFunctionCollectors functionCollectors) {
		this.functionCollectors = functionCollectors;
	}
	
	public boolean isIgnoreIllFieldRow() {
		return ignoreIllFieldRow;
	}

	public void setIgnoreIllFieldRow(boolean ignoreIllFieldRow) {
		this.ignoreIllFieldRow = ignoreIllFieldRow;
	}

	public String getDefaultIllFiledValue() {
		return defaultIllFiledValue;
	}

	public void setDefaultIllFiledValue(String defaultIllFiledValue) {
		this.defaultIllFiledValue = defaultIllFiledValue;
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
