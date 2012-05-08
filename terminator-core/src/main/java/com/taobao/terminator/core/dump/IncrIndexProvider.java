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
				logger.warn("此次全量的起始时间点为   ==> " + startTimeStr + ", 结束时间点为 ==> " + endTimeStr);
			}
			
			if(dataProvider instanceof IncrTimeSupport){
				((IncrTimeSupport) dataProvider).setIncrTime(this.timerManager.justGetTimes().startTime, this.timerManager.justGetTimes().endTime);
			}
		} catch (Throwable e){
			throw new DumpFatalException("TimerManager初始化或者或者读取时间异常", e);
		}
		
		try {
			dataProvider.init();
		} catch (Throwable e){
			throw new DumpFatalException("DataProvider初始化异常", e);
		}		
	}
	
	@Override
	protected void afterDump(boolean isFatal,int sucCount,int failedCount)throws DumpFatalException {
		if(sucCount == 0 && failedCount == 0){
			logger.warn("此次增量的数据为0个，也就是说在此期间没有数据变化,故不重置增量时间.");
			return;
		}
		
		try {
			commintWithoutOptimize();
		} catch (Exception e1) {
			logger.error("增量后提交（不进行优化）索引失败",e1);
		}
		
		try {
			this.timerManager.resetTimes();
		} catch (TimeManageException e) {
			throw new DumpFatalException("TimerManager重置时间异常", e);
		}
	}

	@Override
	protected void processDoc(Map<String, String> row, SolrCore solrCore) throws Exception {
		if (this.interrupted.get()) {
			logger.warn("处理增量document过程被中断，关闭updateHandler");
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
