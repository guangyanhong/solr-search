package com.taobao.terminator.core.exception;

public class SolrXmlParseException extends RuntimeException {
	private static final long serialVersionUID = 1522535877261945974L;

	public SolrXmlParseException(){
		super();
	}
	
	public SolrXmlParseException(String msg){
		super(msg);
	}
	
	public SolrXmlParseException(String msg, Throwable e){
		super(msg, e);
	}
	
	public SolrXmlParseException(Throwable e){
		super(e);
	}
}
