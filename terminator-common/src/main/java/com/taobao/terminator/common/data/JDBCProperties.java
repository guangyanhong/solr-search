package com.taobao.terminator.common.data;

import java.util.Properties;

/**
 * ��չ{ @link Properties }��ʹ֮����֧��һЩ����Դ��Ĭ�ϲ������õȲ���
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
	 * ���ڴ洢DataSource�ĸ�������
	 * @param type ����DBMS����
	 */
	public JDBCProperties(DBMSType type) {
		super();
		this.type = type;
		this.init();
	}
	
	/**
	 * ���ڴ洢DataSource�ĸ�������
	 * @param type ����DBMS����
	 * @param defaults Ĭ��ֵ
	 */
	public JDBCProperties(DBMSType type, Properties defaults) {
		super(defaults);
		this.type = type;
		this.init();
	}
	
	/**
	 * �����û����õ�MaxRowֵ���������ת��Ϊint���򷵻�Ĭ��ֵ
	 * @return maxRow��ֵ
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
	 * �����û����õ�queryTimeoutֵ���������ת��Ϊint���򷵻�Ĭ��ֵ
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
	 * �����û����õ�fetchSizeֵ���������ת��Ϊint���򷵻�Ĭ��ֵ
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
	 * ���ر�Properties��DBMS����
	 * @return ��Properties��DBMS����
	 */
	public DBMSType getType() {
		return type;
	}
	
	/**
	 * ���ر�Properties��DBMS����
	 * @return ��Properties��DBMS����
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
	 * DBMS���ͣ�{@value DBMSType#mysql}ΪMySQL��Ĭ��ֵ����{@value DBMSType#oracle}ΪOracle
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
	 * ��ʼ�����ã��Ȱ�Ĭ��ֵ����DBMS���ͼӽ�ȥ
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
