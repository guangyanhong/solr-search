package com.taobao.terminator.client.index.data;

import java.util.List;
import java.util.Map;

/**
 * 多库多表的DataProvider
 * 
 * @author yusen
 */
public class MultiDBDataProvider implements DataProvider{
	private List<MultiTableDataProvider> dataProviders = null;
	private int currentProviderIndex = 0;
	private int totalFetchedNum = 0;

	@Override
	public void init() throws DataProviderException {
		if(dataProviders == null || dataProviders.isEmpty()){
			throw new DataProviderException("MultiDBDataProvider 初始化失败,dataProviders为空.");
		}
	}
	
	@Override
	public void close() throws DataProviderException {
		for(MultiTableDataProvider p : dataProviders){
			p.close();
		}
		currentProviderIndex = 0;
		totalFetchedNum = 0;
	}

	@Override
	public int getTotalFetchedNum() {
		return totalFetchedNum;
	}

	@Override
	public boolean hasNext() throws DataProviderException {
		MultiTableDataProvider currentDataProvider = this.dataProviders.get(currentProviderIndex);
		if(!currentDataProvider.isInited()){
			currentDataProvider.init();
		}
		
		boolean currentSigleDataProviderHasNext = currentDataProvider.hasNext();
		if(currentSigleDataProviderHasNext){
			totalFetchedNum ++;
			return true;
		}else{
			if(logger.isDebugEnabled()){
				logger.debug("当前的 TableDataProvider 的索引为 ==> [" + currentProviderIndex +"].");
			}
			if(currentProviderIndex >= (dataProviders.size() -1)){
				currentDataProvider.close();
				return false;
			}else{
				logger.warn("当前的TableDataProvider [" + currentProviderIndex + "] 的数据走到了尽头,尝试移到下一个TableDataProvider对象进行数据导出.");
				currentDataProvider.close();
				currentProviderIndex ++;
				return this.hasNext();
			}
		}
	}

	@Override
	public Map<String, String> next() throws DataProviderException {
		return this.dataProviders.get(currentProviderIndex).next();
	}

	public List<MultiTableDataProvider> getDataProviders() {
		return dataProviders;
	}

	public void setDataProviders(List<MultiTableDataProvider> dataProviders) {
		this.dataProviders = dataProviders;
	}
}
