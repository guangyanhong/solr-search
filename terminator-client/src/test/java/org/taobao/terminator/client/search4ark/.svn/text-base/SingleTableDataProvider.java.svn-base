package org.taobao.terminator.client.search4ark;

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

import com.taobao.terminator.client.index.data.DataProvider;
import com.taobao.terminator.client.index.data.DataProviderException;
import com.taobao.terminator.client.index.data.SqlFunction;
import com.taobao.terminator.client.index.data.SqlFunctionCollectors;

/**
 * 单库单表的DataProvider
 * 
 * @author yusen
 *
 */
public class SingleTableDataProvider implements DataProvider{
	
	private DataSource dataSource;
	private Connection connnection;
	private String     sql;
	private String     serviceName;
	private Statement  statement;
	private ResultSet  resultSet;
	private int        columCount;
	private int        totalFetchedNum = 0;
	
	private SqlFunctionCollectors sqlFuncs;
	private ResultSetMetaData     metaData;
	private AtomicBoolean         isInited = new AtomicBoolean(false);
	private List<SqlFunction>     plusSqlFuncs = null;
	
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
			statement.setFetchSize(Integer.MIN_VALUE);
			String executeSql = this.buildSql(sql);
			
			logger.warn("执行的SQL == > \n" + executeSql);
			
			resultSet  = statement.executeQuery(executeSql);
			metaData   = resultSet.getMetaData();
			columCount = metaData.getColumnCount();
		}catch(Exception e){
			throw new DataProviderException("[single-init-error]",e);
		}
		isInited.set(true);
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
