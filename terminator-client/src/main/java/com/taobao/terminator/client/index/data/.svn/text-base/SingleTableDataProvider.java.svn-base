package com.taobao.terminator.client.index.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

/**
 * 单库单表的DataProvider
 * 
 * @author yusen
 *
 */
public class SingleTableDataProvider implements DataProvider{
	
	protected DataSource dataSource;
	protected Connection connnection;
	protected String     sql;
	protected String     serviceName;
	protected Statement  statement;
	protected ResultSet  resultSet;
	protected int        columCount;
	protected int        totalFetchedNum = 0;
	
	protected SqlFunctionCollectors sqlFuncs;
	protected ResultSetMetaData     metaData;
	protected AtomicBoolean         isInited = new AtomicBoolean(false);
	protected List<SqlFunction>     plusSqlFuncs = null;
	
	public SingleTableDataProvider(){}
	
	public SingleTableDataProvider(DataSource ds,String sql,String serviceName) throws DataProviderException{
		this.dataSource  = ds;
		this.serviceName = serviceName;
		this.sql         = sql;
	}
	
	public void init()throws DataProviderException{
		if(logger.isDebugEnabled()){
			logger.debug("执行Sql查询.");
		}
		
		if(isInited())
			return;
		
		if(dataSource == null || StringUtils.isBlank(sql)){
			throw new DataProviderException("[single-init-error] DataSource 和 sql属性不能为null或者空,请先注入这两个属性.");
		}
		
		if(sqlFuncs == null){
			sqlFuncs = new SqlFunctionCollectors(serviceName);
			sqlFuncs.initDefaultFunctions();
			if(plusSqlFuncs != null && !plusSqlFuncs.isEmpty()){
				logger.warn("用户自定义了SqlFunctions");
				for(SqlFunction f : plusSqlFuncs){
					if(sqlFuncs.register(f) != null){
						logger.warn("用户自定义的SqlFunction ==> " + f.getPlaceHolderName() + " 覆盖了默认的Function.");
					}
				}
			}
		}
		
		try{
			this.connnection = dataSource.getConnection();
			statement = connnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			String executeSql = this.buildSql(sql);
			logger.warn("执行的SQL == > \n" + executeSql);
			
			this.beforExecuteSql(connnection, statement);
			resultSet  = statement.executeQuery(executeSql);
			metaData   = resultSet.getMetaData();
			columCount = metaData.getColumnCount();
		}catch(Exception e){
			throw new DataProviderException("[single-init-error]",e);
		}
		isInited.set(true);
	}
	
	/**
	 * Statement执行之前的一些预处理，比如参数设置等
	 * 
	 * 客户可自行决定是否覆盖该方法，比如Oracle和MySql的一些不同的Statement的参数设置可以在这个方法里进行设置
	 * @param conn
	 * @param statement
	 * @throws Exception
	 */
	protected void beforExecuteSql(Connection conn, Statement statement) throws Exception {
		statement.setFetchSize(Integer.MIN_VALUE);
	}
	
	protected String buildSql(String sql){
		return sqlFuncs.parseSql(sql);
	}

	public boolean isInited(){
		return this.isInited.get();
	}
	
	@Override
	public boolean hasNext() throws DataProviderException{
		try {
			return resultSet.next();
		} catch (SQLException e) {
			throw new DataProviderException("[single-hasNext-error]",e);
		}
	}

	@Override
	public Map<String, String> next() throws DataProviderException{
		totalFetchedNum ++;
		Map<String,String> row =  new HashMap<String,String>(columCount);
		for(int i = 1; i <= columCount; i++){				
			String key   = null;
			String value = null;
			try {
				key = metaData.getColumnLabel(i);
				value = resultSet.getString(i);
			} catch (SQLException e) {
				throw new DataProviderException("[single-next-error]",e);
			}
			row.put(key, value != null ? value : " ");
		}
		return row;
	}

	@Override
	public void close() throws DataProviderException{
		logger.warn("关闭DataProvider,释放数据库连接资源，状态标志归位.");
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
			if(connnection != null)
				connnection.close();
		} catch (SQLException e) {
			throw new DataProviderException("single-close-error",e);
		} finally {
			resultSet   = null;
			statement   = null;
			connnection = null;
			metaData    = null;
			columCount  = 0;
			totalFetchedNum = 0;
			isInited.set(false);
		}
	}

	@Override
	public int getTotalFetchedNum() {
		return totalFetchedNum;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public SqlFunctionCollectors getSqlFuncs() {
		return sqlFuncs;
	}

	public void setSqlFuncs(SqlFunctionCollectors sqlFuncs) {
		this.sqlFuncs = sqlFuncs;
	}

	public List<SqlFunction> getPlusSqlFuncs() {
		return plusSqlFuncs;
	}

	public void setPlusSqlFuncs(List<SqlFunction> plusSqlFuncs) {
		this.plusSqlFuncs = plusSqlFuncs;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
}
