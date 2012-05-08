package com.taobao.terminator.common.data;

import java.util.Properties;

/**
 * 扩展{ @link Properties }，使之可以支持一些数据源的默认参数设置等操作
 */
public class JDBCProperties extends Properties {
	private static final long serialVersionUID = -7921975974894453161L;
	
	public static final int MYSQL_STATEMENT_MAX_ROW = 50000;
	public static final int MYSQL_STATEMENT_QUERY_TIMEOUT = 3000;
	public static final int MYSQL_STATEMENT_FETCH_SIZE = Integer.MIN_VALUE;
	
	public static final int ORACLE_STATEMENT_MAX_ROW = 50000;
	public static final int ORACLE_STATEMENT_QUERY_TIMEOUT = 3000;
	public static final int ORACLE_STATEMENT_FETCH_SIZE = 5000;
	
	public static final String STATEMENT_MAX_ROW = "maxRow";
	public static final String STATEMENT_QUERY_TIMEOUT = "queryTimeout";
	public static final String STATEMENT_FETCH_SIZE = "fetchSize";
	
	public static final String MYSQL_DBMS_NAME = "MySQL";
	public static final String ORACLE_DBMS_NAME = "Oracle";
	
	protected DBMSType type;

	/**
	 * 用于存储DataSource的附加属性
	 * @param type 所用DBMS类型
	 */
	public JDBCProperties(DBMSType type) {
		super();
		this.type = type;
		this.init();
	}
	
	/**
	 * 用于存储DataSource的附加属性
	 * @param type 所用DBMS类型
	 * @param defaults 默认值
	 */
	public JDBCProperties(DBMSType type, Properties defaults) {
		super(defaults);
		this.type = type;
		this.init();
	}
	
	/**
	 * 返回用户设置的MaxRow值，如果不能转化为int，则返回默认值
	 * @return maxRow的值
	 */
	public int getStatementMaxRow() {
		String strMaxRow = this.getProperty(STATEMENT_MAX_ROW);
		int maxRow = 0;
		try {
			maxRow = Integer.parseInt(strMaxRow);
		} catch(NumberFormatException nfe) {
			maxRow = this.type==DBMSType.MYSQL? MYSQL_STATEMENT_MAX_ROW: ORACLE_STATEMENT_MAX_ROW;
		}
		return maxRow;
	}
	
	/**
	 * 返回用户设置的queryTimeout值，如果不能转化为int，则返回默认值
	 * @return
	 */
	public int getStatementQueryTimeout() {
		String strQueryTimeout = this.getProperty(STATEMENT_QUERY_TIMEOUT);
		int queryTimeout = 0;
		try {
			queryTimeout = Integer.parseInt(strQueryTimeout);
		} catch(NumberFormatException nfe) {
			queryTimeout = this.type==DBMSType.MYSQL? MYSQL_STATEMENT_QUERY_TIMEOUT: ORACLE_STATEMENT_QUERY_TIMEOUT;
		}
		return queryTimeout;
	}
	
	/**
	 * 返回用户设置的fetchSize值，如果不能转化为int，则返回默认值
	 * @return
	 */
	public int getStatementFetchSize() {
		String strFetchSize = this.getProperty(STATEMENT_FETCH_SIZE);
		int fetchSize = 0;
		try {
			fetchSize = Integer.parseInt(strFetchSize);
		} catch(NumberFormatException nfe) {
			fetchSize = this.type==DBMSType.MYSQL? MYSQL_STATEMENT_FETCH_SIZE: ORACLE_STATEMENT_FETCH_SIZE;
		}
		fetchSize = fetchSize < 0 ? Integer.MIN_VALUE : fetchSize;
		this.put(STATEMENT_FETCH_SIZE, fetchSize);
		return fetchSize;
	}
	
	/**
	 * 返回本Properties的DBMS类型
	 * @return 本Properties的DBMS类型
	 */
	public DBMSType getType() {
		return type;
	}
	
	/**
	 * 返回本Properties的DBMS名称
	 * @return 本Properties的DBMS名称
	 */
	public String getDBMSName() {
		switch(this.type) {
		case ORACLE:
			return ORACLE_DBMS_NAME;
		default:
			return MYSQL_DBMS_NAME;
		}
	}

	/**
	 * DBMS类型，{@value DBMSType#mysql}为MySQL（默认值），{@value DBMSType#oracle}为Oracle
	 */
	public static enum DBMSType {
		MYSQL, ORACLE;
		public static DBMSType getDBMSTpye(String dbName) {
			if("oracle".equalsIgnoreCase(dbName))
				return ORACLE;
			else
				return MYSQL;
		}
	}
	
	/**
	 * 初始化设置，先把默认值根据DBMS类型加进去
	 */
	private void init() {
		if(this.type == DBMSType.ORACLE) {
			this.put(STATEMENT_MAX_ROW, ORACLE_STATEMENT_MAX_ROW);
			this.put(STATEMENT_QUERY_TIMEOUT, ORACLE_STATEMENT_QUERY_TIMEOUT);
			this.put(STATEMENT_FETCH_SIZE, ORACLE_STATEMENT_FETCH_SIZE);
		} else {
			this.put(STATEMENT_MAX_ROW, MYSQL_STATEMENT_MAX_ROW);
			this.put(STATEMENT_QUERY_TIMEOUT, MYSQL_STATEMENT_QUERY_TIMEOUT);
			this.put(STATEMENT_FETCH_SIZE, MYSQL_STATEMENT_FETCH_SIZE);
		}
	}
}
