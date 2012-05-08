package com.taobao.terminator.core.dump;

import java.util.Date;
import java.util.Map;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.data.DataProvider;
import com.taobao.terminator.common.data.PlusSqlFunctionRegisterable;
import com.taobao.terminator.common.data.filter.GroupFilter;
import com.taobao.terminator.common.data.processor.DataProcessor;
import com.taobao.terminator.common.data.processor.DeletionDataProcessor;
import com.taobao.terminator.common.data.sql.NowFunction;

public class FullIndexProvider extends AbstractIndexProvider {
	private Date startTime = null;

	public FullIndexProvider() {
		super();
	}

	public FullIndexProvider(DataProvider dataProvider, DataProcessor dataProcessor, GroupFilter groupFilter, SolrCore solrCore) {
		super(dataProvider, dataProcessor, groupFilter, solrCore);
	}

	@Override
	protected void beforeDump() throws DumpFatalException{
		if (dataProvider instanceof PlusSqlFunctionRegisterable) {
			((PlusSqlFunctionRegisterable) dataProvider) .registerSqlFunction(new NowFunction());
		}
		
		this.startTime = new Date();
		logger.warn("此次全量的起始时间点为   ==> {" + TerminatorCommonUtils.formatDate(this.startTime) + "} 全量成功后会将此时间重写入增量时间控制文件.");
		
		try {
			dataProvider.init();
		} catch (Throwable e) {
			throw new DumpFatalException("DataProvider初始化失败", e);
		}
	}

	@Override
	protected void afterDump(boolean isFatal,int sucCount,int failedCount)throws DumpFatalException{
		if(isFatal){
			throw new DumpFatalException("Dump过程中出现了严重错误.");
		}
		
		if(sucCount == 0){
			throw new DumpFatalException("整个Dump过程中总共才取出数据0条，视此次Dump为失败的.");
		}
		
		try {
			commintWithOptimize();
		} catch (Exception e) {
			throw new DumpFatalException("全量完毕后提交且优化索引失败", e);
		}
	}

	@Override
	protected void processDoc(Map<String, String> row, SolrCore solrCore) throws Exception{
		if(row.containsKey(DeletionDataProcessor.DELETION_KEY)) {
			return ;
		}
		AddUpdateCommand addCmd = SolrCommandBuilder.generateAddCommand(row, this.solrCore.getSchema());
		addCmd.allowDups = true;
		this.solrCore.getUpdateHandler().addDoc(addCmd);
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
}

