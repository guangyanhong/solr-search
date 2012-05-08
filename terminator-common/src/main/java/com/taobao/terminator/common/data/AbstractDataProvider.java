package com.taobao.terminator.common.data;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * DataProvider的抽象实现，完成了一些必要的方法
 */
public abstract class AbstractDataProvider implements DataProvider{
	protected Log logger = LogFactory.getLog(AbstractDataProvider.class);
	protected boolean isClosed = false;
	protected boolean isInited = false;
	
	
	@Override
	public void init() throws Exception {
		if(isInited) {
			return;
		}
		try{
			this.doInit();
		}  finally{
			isInited = true;
			isClosed = false;
		}
	}

	@Override
	public void close() throws Exception {
		if(!isClosed){
			try{
				this.doClose();
			} finally{
				isClosed = true;
				isInited = false;
			}
		}
	}
	
	public boolean isInited(){
		return false;
	}

	protected abstract void doClose() throws Exception;
	protected abstract void doInit() throws Exception;
	
	public abstract boolean hasNext() throws Exception;
	public abstract Map<String, String> next() throws Exception;
}
