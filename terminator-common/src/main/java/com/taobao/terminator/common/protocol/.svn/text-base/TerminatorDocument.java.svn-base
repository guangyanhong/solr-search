package com.taobao.terminator.common.protocol;

import java.util.HashMap;

public class TerminatorDocument extends HashMap<String,String>{
	public static final String BOOST_KEY_NAME = "$boost";
	private static final long serialVersionUID = 5801535511116834899L;
	
	public TerminatorDocument(){
		this.put(BOOST_KEY_NAME, "1.0f");
	}

	public TerminatorDocument addField(String name,String value){
		this.put(name, value);
		return this;
	}
	
	public void setBoost(float boost){
		this.put(BOOST_KEY_NAME, String.valueOf(boost));
	}
}
