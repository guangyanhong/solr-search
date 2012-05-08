package com.taobao.terminator.common.data;

/**
 * ָ����DataSource name�����ڽ��׳����쳣
 */
public class DataSourceDefinationException extends RuntimeException {
	private static final long serialVersionUID = 237117354639144059L;
	
	public DataSourceDefinationException() {
		super();
	}
	
	public DataSourceDefinationException(String message) {
		super(message);
	}
	
	public DataSourceDefinationException(String message, Throwable t) {
		super(message, t);
	}
	
	public DataSourceDefinationException(Throwable t) {
		super(t);
	}
}
