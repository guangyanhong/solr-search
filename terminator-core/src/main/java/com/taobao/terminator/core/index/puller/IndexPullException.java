package com.taobao.terminator.core.index.puller;

public class IndexPullException extends Exception{

	private static final long serialVersionUID = -6082227511932870570L;

	public IndexPullException() {
		super();
	}

	public IndexPullException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexPullException(String message) {
		super(message);
	}

	public IndexPullException(Throwable cause) {
		super(cause);
	}
}
