package com.taobao.terminator.common.data;

import java.text.DecimalFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ModTableRouter implements TableRouter {
	
	protected Log logger = LogFactory.getLog(DefaultTableDescriptionParser.class);
	protected DecimalFormat format;
	
	protected int groupNum;
	
	public ModTableRouter() {
		format = new DecimalFormat("_0000");
	}
	
	public ModTableRouter(int groupNum) {
		this();
		this.groupNum = groupNum;
	}
	
	public ModTableRouter(int groupNum, DecimalFormat format) {
		this.groupNum = groupNum;
		this.format = format;
	}

	@Override
	public String getSubtableDesc(String key) throws Exception {
		long id = 0;
		try {
			id = Long.parseLong(key);
		} catch(NumberFormatException nfe) {
			logger.error("传入参数错误，不能转化为long类型", nfe);
			throw nfe;
		}
		
		return format.format(id % groupNum);
	}

	public DecimalFormat getFormat() {
		return format;
	}

	public void setFormat(DecimalFormat format) {
		this.format = format;
	}

	public int getGroupNum() {
		return groupNum;
	}

	public void setGroupNum(int groupNum) {
		this.groupNum = groupNum;
	}

}
