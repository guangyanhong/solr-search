package com.taobao.terminator.common.constant;

public enum IndexType {
	FULL("full"),INCREMENT("increment"),INSTANT("instant");
	
	private String value;
	
	IndexType(String value){
		this.value = value;
	}
	
	public String getValue(){
		return this.value;
	}
}
