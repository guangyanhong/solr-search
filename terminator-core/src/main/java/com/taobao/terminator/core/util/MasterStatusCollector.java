package com.taobao.terminator.core.util;

import java.util.HashMap;
import java.util.Map;

import com.taobao.terminator.core.service.MasterStatus;

public class MasterStatusCollector {
	private static Map<String,MasterStatus> context = new HashMap<String,MasterStatus>();
	
	public static void register(String name,MasterStatus masterStatus){
		context.put(name, masterStatus);
	}
	
	public static MasterStatus get(String name){
		return context.get(name);
	}
}
