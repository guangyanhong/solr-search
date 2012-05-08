package com.taobao.terminator.core.dump;

public class InitException extends RuntimeException{

	private static final long serialVersionUID = 2257193800230421360L;

	public InitException() {
		super();
	}

	public InitException(String message, Throwable cause) {
		super(message, cause);
	}

	public InitException(String message) {
		super(message);
	}

	public InitException(Throwable cause) {
		super(cause);
	}
}
