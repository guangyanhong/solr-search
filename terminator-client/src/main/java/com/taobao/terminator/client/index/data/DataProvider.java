package com.taobao.terminator.client.index.data;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public interface DataProvider{
	Log logger = LogFactory.getLog(DataProvider.class);
	
	public void init() throws DataProviderException;
	
	public boolean hasNext()throws DataProviderException;

    public Map<String,String> next()throws DataProviderException;

	public void close()throws DataProviderException;
	
	public int getTotalFetchedNum();
}
