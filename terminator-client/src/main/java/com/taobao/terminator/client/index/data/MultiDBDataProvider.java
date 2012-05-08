package com.taobao.terminator.client.index.data;

import java.util.List;
import java.util.Map;

/**
 * ������DataProvider
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
			throw new DataProviderException("MultiDBDataProvider ��ʼ��ʧ��,dataProvidersΪ��.");
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
				logger.debug("��ǰ�� TableDataProvider ������Ϊ ==> [" + currentProviderIndex +"].");
			}
			if(currentProviderIndex >= (dataProviders.size() -1)){
				currentDataProvider.close();
				return false;
			}else{
				logger.warn("��ǰ��TableDataProvider [" + currentProviderIndex + "] �������ߵ��˾�ͷ,�����Ƶ���һ��TableDataProvider����������ݵ���.");
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
