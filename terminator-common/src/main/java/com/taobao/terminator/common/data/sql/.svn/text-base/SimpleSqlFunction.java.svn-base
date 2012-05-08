package com.taobao.terminator.common.data.sql;

public final class SimpleSqlFunction implements SqlFunction {
	
	private String placeHolderName;
	private String value;
	
	public SimpleSqlFunction(){}
	
	public SimpleSqlFunction(String placeHolderName, String value) {
		this.placeHolderName = placeHolderName;
		this.value           = value;
	}

	@Override
	public String getPlaceHolderName() {
		return this.placeHolderName;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setPlaceHolderName(String placeHolderName) {
		this.placeHolderName = placeHolderName;
	}
}
