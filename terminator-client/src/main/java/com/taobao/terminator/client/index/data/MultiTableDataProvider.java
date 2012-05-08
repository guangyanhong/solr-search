package com.taobao.terminator.client.index.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

/**
 * 单库多表的DataProvider
 * 
 * @author yusen
 *
 */
public class MultiTableDataProvider implements DataProvider{
	protected static final String tableSuffixPlaceHolder = "$tablename$"; //分表的后缀的占位符
	
	protected DataSource dataSource   = null;
	protected String     subtableDesc = null;
	protected String     baseSql      = null;
	protected int        totalFetchedNum = 0;
	protected String     serviceName  = null;
	
	protected SubtableDescParser      parser        = null;
	protected List<SingleTableDataProvider> dataProviders = null;
	protected AtomicBoolean isInited = new AtomicBoolean(false);
	protected List<SqlFunction>     plusSqlFuncs = null;
	
	protected int currentDataProviderIndex = 0;
	
	public MultiTableDataProvider(){}
	
	public MultiTableDataProvider(DataSource dataSource,String baseSql,String serviceName)throws DataProviderException {
		this.dataSource  = dataSource;
		this.baseSql     = baseSql;
		this.serviceName = serviceName;
	}

	@Override
	public void init() throws DataProviderException {
		if(this.isInited()){
			return;
		}
		
		logger.warn("初始化MultiTableDatProvider,baseSql ==> " + baseSql);
		if(parser == null){
			parser = new DefaultSubtableDescParser();
		}
		
		if(dataProviders == null){
			
			List<String> subtableList = null;
			try {
				subtableList = parser.parse(subtableDesc);
			} catch (SubtableDescParseException e) {
				throw new DataProviderException("解析分表规则失败.",e);
			}
			
			dataProviders = new ArrayList<SingleTableDataProvider>(subtableList.size());
			int i = 0;
			for(String subtable : subtableList){
				String sql = baseSql.replace(tableSuffixPlaceHolder, subtable);
				logger.warn("[" + (++i)+"] 创建TableDataProvider,替换分表名后缀 [" + tableSuffixPlaceHolder + "] 后的 sql ==> " + sql);
				SingleTableDataProvider dataProvider =  new SingleTableDataProvider(dataSource,sql,serviceName);
				dataProvider.setPlusSqlFuncs(plusSqlFuncs);
				dataProviders.add(dataProvider);
			}
		}
		isInited.set(true);
		currentDataProviderIndex = 0;
		totalFetchedNum = 0;
	}
	
	public boolean isInited(){
		return isInited.get();
	}
	
	@Override
	public void close() throws DataProviderException{
		for(SingleTableDataProvider p :dataProviders){
			p.close();
		}
		currentDataProviderIndex = 0;
		totalFetchedNum = 0;
	}

	@Override
	public int getTotalFetchedNum() {
		return totalFetchedNum;
	}

	@Override
	public boolean hasNext()throws DataProviderException {
		SingleTableDataProvider currentDataProvider = this.dataProviders.get(currentDataProviderIndex);
		if(!currentDataProvider.isInited()){
			currentDataProvider.init();
		}
		
		boolean currentSigleDataProviderHasNext = currentDataProvider.hasNext();
		if(currentSigleDataProviderHasNext){
			totalFetchedNum ++;
			return true;
		}else{
			if(logger.isDebugEnabled()){
				logger.debug("当前的 TableDataProvider 的索引为 ==> [" + currentDataProviderIndex +"].");
			}
			if(currentDataProviderIndex >= (dataProviders.size() -1)){
				currentDataProvider.close();
				return false;
			}else{
				logger.warn("当前的TableDataProvider [" + currentDataProviderIndex + "] 的数据走到了尽头,尝试移到下一个TableDataProvider对象进行数据导出.");
				currentDataProvider.close();
				currentDataProviderIndex ++;
				return this.hasNext();
			}
		}
	}

	@Override
	public Map<String, String> next() throws DataProviderException{
		return this.dataProviders.get(currentDataProviderIndex).next();
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public String getSubtableDesc() {
		return subtableDesc;
	}

	public void setSubtableDesc(String subtableDesc) {
		this.subtableDesc = subtableDesc;
	}

	public String getBaseSql() {
		return baseSql;
	}

	public void setBaseSql(String baseSql) {
		this.baseSql = baseSql;
	}

	public SubtableDescParser getParser() {
		return parser;
	}

	public void setParser(SubtableDescParser parser) {
		this.parser = parser;
	}

	public List<SingleTableDataProvider> getDataProviders() {
		return dataProviders;
	}

	public void setDataProviders(List<SingleTableDataProvider> dataProviders) {
		this.dataProviders = dataProviders;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public List<SqlFunction> getPlusSqlFuncs() {
		return plusSqlFuncs;
	}

	public void setPlusSqlFuncs(List<SqlFunction> plusSqlFuncs) {
		this.plusSqlFuncs = plusSqlFuncs;
	}

	public int getCurrentDataProviderIndex() {
		return currentDataProviderIndex;
	}
}
