package com.taobao.terminator.client.index.data.procesor;

public class DataProcessException extends Exception {

	private static final long serialVersionUID = -2827090620397658924L;

	public DataProcessException() {
		super();
	}

	public DataProcessException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataProcessException(String message) {
		super(message);
	}

	public DataProcessException(Throwable cause) {
		super(cause);
	}
}
