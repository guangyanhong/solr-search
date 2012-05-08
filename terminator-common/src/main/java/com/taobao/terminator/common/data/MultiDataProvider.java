package com.taobao.terminator.common.data;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 多个DataProvider组合运行的场景可通过这个对象进行聚合
 * 
 * @author yusen
 * deprecated 不支持SqlFunction注入到subDataProvider 推荐 SFSupportMultiDataProvider
 */
public class MultiDataProvider extends AbstractDataProvider{
	protected static Log logger = LogFactory.getLog(MultiDataProvider.class);
	
	protected List<DataProvider> 	 dataProviders;
	protected int 					 currentDataProviderIndex = 0;
	
	@Override
	protected void doInit() throws Exception {}

	@Override
	protected void doClose() throws Exception {
		try{
			for(DataProvider p :dataProviders) {
				p.close();
			}
		} finally{
			currentDataProviderIndex = 0;
		}
	}

	@Override
	public boolean hasNext() throws Exception {
		DataProvider currentDataProvider = this.dataProviders.get(currentDataProviderIndex);
		currentDataProvider.init();
		
		boolean currentSigleDataProviderHasNext = currentDataProvider.hasNext();
		if(currentSigleDataProviderHasNext) {
			return true;
		} else {
			if(logger.isDebugEnabled()) {
				logger.debug("当前的 TableDataProvider 的索引为 ==> [" + currentDataProviderIndex +"].");
			}
			if(currentDataProviderIndex >= (dataProviders.size() -1)) {
				currentDataProvider.close();
				return false;
			} else {
				logger.warn("当前的TableDataProvider [" + currentDataProviderIndex + "] 的数据走到了尽头,尝试移到下一个TableDataProvider对象进行数据导出.");
				currentDataProvider.close();
				currentDataProviderIndex ++;
				return this.hasNext();
			}
		}
	}

	@Override
	public Map<String, String> next() throws Exception {
		return this.dataProviders.get(currentDataProviderIndex).next();
	}

	public List<DataProvider> getDataProviders() {
		return dataProviders;
	}

	public void setDataProviders(List<DataProvider> dataProviders) {
		this.dataProviders = dataProviders;
	}

	public int getCurrentDataProviderIndex() {
		return currentDataProviderIndex;
	}

	public void setCurrentDataProviderIndex(int currentDataProviderIndex) {
		this.currentDataProviderIndex = currentDataProviderIndex;
	}
}
