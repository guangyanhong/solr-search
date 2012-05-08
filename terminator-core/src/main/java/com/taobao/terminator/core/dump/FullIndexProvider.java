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
		logger.warn("�˴�ȫ������ʼʱ���Ϊ   ==> {" + TerminatorCommonUtils.formatDate(this.startTime) + "} ȫ���ɹ���Ὣ��ʱ����д������ʱ������ļ�.");
		
		try {
			dataProvider.init();
		} catch (Throwable e) {
			throw new DumpFatalException("DataProvider��ʼ��ʧ��", e);
		}
	}

	@Override
	protected void afterDump(boolean isFatal,int sucCount,int failedCount)throws DumpFatalException{
		if(isFatal){
			throw new DumpFatalException("Dump�����г��������ش���.");
		}
		
		if(sucCount == 0){
			throw new DumpFatalException("����Dump�������ܹ���ȡ������0�����Ӵ˴�DumpΪʧ�ܵ�.");
		}
		
		try {
			commintWithOptimize();
		} catch (Exception e) {
			throw new DumpFatalException("ȫ����Ϻ��ύ���Ż�����ʧ��", e);
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

