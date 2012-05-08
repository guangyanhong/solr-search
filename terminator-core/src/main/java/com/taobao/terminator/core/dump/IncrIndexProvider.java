package com.taobao.terminator.core.dump;

import java.util.Map;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;

import com.taobao.terminator.common.data.DataProvider;
import com.taobao.terminator.common.data.IncrTimeSupport;
import com.taobao.terminator.common.data.PlusSqlFunctionRegisterable;
import com.taobao.terminator.common.data.filter.GroupFilter;
import com.taobao.terminator.common.data.processor.DataProcessor;
import com.taobao.terminator.common.data.processor.DeletionDataProcessor;
import com.taobao.terminator.common.data.sql.NowFunction;
import com.taobao.terminator.common.data.sql.SimpleSqlFunction;
import com.taobao.terminator.common.data.sql.SqlUtils;
import com.taobao.terminator.common.data.timer.TimeManageException;
import com.taobao.terminator.common.data.timer.TimerManager;

public class IncrIndexProvider extends AbstractIndexProvider {
	protected TimerManager timerManager ;

	public IncrIndexProvider() {
		super();
	}

	public IncrIndexProvider(DataProvider dataProvider, DataProcessor dataProcessor, GroupFilter groupFilter, SolrCore solrCore,TimerManager timerManager) {
		super(dataProvider, dataProcessor, groupFilter, solrCore);
		this.timerManager = timerManager;
	}

	@Override
	protected void beforeDump() throws DumpFatalException {
		try {
			this.timerManager.initTimes();
			if(dataProvider instanceof PlusSqlFunctionRegisterable) {
				PlusSqlFunctionRegisterable regi = ((PlusSqlFunctionRegisterable)dataProvider);
				
				String startTimeStr = SqlUtils.parseDate(this.timerManager.justGetTimes().startTime);
				String endTimeStr   = SqlUtils.parseDate(this.timerManager.justGetTimes().endTime);
				
				regi.unregisterAll();
				
				regi.registerSqlFunction(new NowFunction());
				regi.registerSqlFunction(new SimpleSqlFunction("startDate", startTimeStr));
				regi.registerSqlFunction(new SimpleSqlFunction("endDate", endTimeStr));
				regi.registerSqlFunction(new SimpleSqlFunction("lastModified", startTimeStr));
				logger.warn("�˴�ȫ������ʼʱ���Ϊ   ==> " + startTimeStr + ", ����ʱ���Ϊ ==> " + endTimeStr);
			}
			
			if(dataProvider instanceof IncrTimeSupport){
				((IncrTimeSupport) dataProvider).setIncrTime(this.timerManager.justGetTimes().startTime, this.timerManager.justGetTimes().endTime);
			}
		} catch (Throwable e){
			throw new DumpFatalException("TimerManager��ʼ�����߻��߶�ȡʱ���쳣", e);
		}
		
		try {
			dataProvider.init();
		} catch (Throwable e){
			throw new DumpFatalException("DataProvider��ʼ���쳣", e);
		}		
	}
	
	@Override
	protected void afterDump(boolean isFatal,int sucCount,int failedCount)throws DumpFatalException {
		if(sucCount == 0 && failedCount == 0){
			logger.warn("�˴�����������Ϊ0����Ҳ����˵�ڴ��ڼ�û�����ݱ仯,�ʲ���������ʱ��.");
			return;
		}
		
		try {
			commintWithoutOptimize();
		} catch (Exception e1) {
			logger.error("�������ύ���������Ż�������ʧ��",e1);
		}
		
		try {
			this.timerManager.resetTimes();
		} catch (TimeManageException e) {
			throw new DumpFatalException("TimerManager����ʱ���쳣", e);
		}
	}

	@Override
	protected void processDoc(Map<String, String> row, SolrCore solrCore) throws Exception {
		if (this.interrupted.get()) {
			logger.warn("��������document���̱��жϣ��ر�updateHandler");
			this.solrCore.getUpdateHandler().close();
			return;
		}
		if(row.containsKey(DeletionDataProcessor.DELETION_KEY)) {
			this.solrCore.getUpdateHandler().delete(SolrCommandBuilder.generateDeleteCommand(row));
		}else{
			AddUpdateCommand addCmd = SolrCommandBuilder.generateAddCommand(row, this.solrCore.getSchema());
			addCmd.allowDups = false;
			this.solrCore.getUpdateHandler().addDoc(addCmd);
		}
	}

	public TimerManager getTimerManager() {
		return timerManager;
	}

	public void setTimerManager(TimerManager timerManager) {
		this.timerManager = timerManager;
	}
}
