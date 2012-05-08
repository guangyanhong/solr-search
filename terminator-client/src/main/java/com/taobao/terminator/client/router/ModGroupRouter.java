package com.taobao.terminator.client.router;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 该类利用切分字段对Shard总数取模的方式来获得一条数据应该被分发到哪个shard
 * @author tianxiao
 *
 */
public class ModGroupRouter extends AbstractGroupRouter{

	private String shardKey;
	private static final Log logger = LogFactory.getLog(ModGroupRouter.class);
	
	public ModGroupRouter(){}
	public ModGroupRouter(String shardKey){
		this.shardKey = shardKey;
	}
	
	public String getGroupName(Map<String,String> rowData) {
		String value = rowData.get(shardKey);
		long longVal = getValue(value);
		return String.valueOf((int)longVal % serviceConfig.getGroupNum());
	}

	private long getValue(String value){
		if(value == null){
			RuntimeException e = new IllegalArgumentException("Can't not found shardKey in input row data!");
			logger.error("在输入的一行数据中无法找到shardKey", e);
			throw e;
		}
		
		try{
			return Long.parseLong(value);
		} catch(NumberFormatException e){
			logger.error("切分字段的值无法转换成Long型！", e);
			throw new IllegalArgumentException("Can't convert the shard-key value to Long.", e);
		}
	}
	
	public String getShardKey() {
		return shardKey;
	}

	public void setShardKey(String shardKey) {
		this.shardKey = shardKey;
	}
}
