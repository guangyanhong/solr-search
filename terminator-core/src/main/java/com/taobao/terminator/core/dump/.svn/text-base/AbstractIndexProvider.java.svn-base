package com.taobao.terminator.core.dump;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;

import com.taobao.terminator.common.data.DataProvider;
import com.taobao.terminator.common.data.filter.GroupFilter;
import com.taobao.terminator.common.data.processor.DataProcessor;
import com.taobao.terminator.common.data.processor.DataProcessor.ResultCode;

public abstract class AbstractIndexProvider implements IndexProvider{
	protected static Log logger = LogFactory.getLog(IndexProvider.class);
	
	protected DataProvider  dataProvider;
	protected DataProcessor dataProcessor;
	protected GroupFilter   groupFilter;
	protected SolrCore      solrCore;
	protected String        coreName;
	
	protected AtomicBoolean isDumping ;
	protected AtomicBoolean interrupted;
	
	protected Timer timer = new Timer(Timer.SECOND);
	protected Map<String, String> row;
	
	public AbstractIndexProvider(){
		this.initFields();
	}

	public AbstractIndexProvider(DataProvider dataProvider, DataProcessor dataProcessor, GroupFilter groupFilter, SolrCore solrCore) {
		super();
		this.dataProvider  = dataProvider;
		this.dataProcessor = dataProcessor;
		this.groupFilter   = groupFilter;
		this.solrCore      = solrCore;
		this.coreName      = solrCore.getName();
		this.initFields();
	}
	
	protected void initFields() {
		this.isDumping 	   = new AtomicBoolean(false);
		this.interrupted   = new AtomicBoolean(false);
	}

	@Override
	public void dump() throws DumpFatalException {
		logger.warn("["+ coreName + "] 开始Dump过程.");
		
		//每次调用dump时都把中断选项设置为false
		this.interrupted.set(false);
		if(isDumping.get()){
			throw new DumpFatalException("["+ coreName + "] 有Dump任务正在进行中,拒绝此次Dump请求.");
		}
		isDumping.set(true);
		
		try{
			this.beforeDump();
		} catch(Throwable e){
			isDumping.set(false);
			logger.error("["+ coreName + "] 调用beforeDump方法异常",e);
			DumpFatalException ee =  (e instanceof DumpFatalException) ? (DumpFatalException)e : new DumpFatalException(e);
			throw ee;
		}
		
		this.timer.start();
		
		int sucCount    = 0; 
		int failedCount = 0;
		boolean isFatal   = false;
		
		try {
			while(dataProvider.hasNext() && !this.interrupted.get()) {
				row = null; 
				try {
					row = dataProvider.next();
				} catch(Exception e) {
					failedCount++;
					logger.error("["+ coreName + "] 获取下一条数据异常,当前失败的总数为  ==> " + failedCount + "条，总记录数为 ==>" + sucCount,e);
					continue;
				}
				
				if(row == null){
					failedCount++;
					continue;
				}
				
				if(groupFilter == null || groupFilter.accept(row)){
					if(dataProcessor != null){
						try {
							ResultCode rs = dataProcessor.process(row);
							if(rs == null || !rs.isSuc()){
								logger.debug("["+ coreName + "] DataProcessor摒弃数据,原因  ==>" + (rs != null ? rs.toString():" ResultCode is nulll ") + "   \n数据描述 ==> " + row.toString());
								failedCount ++;
								continue;
							}
						} catch (Exception e) {
							logger.error("["+ coreName + "] DataProcessor处理数据异常,忽略此条数据,data ==> " + row.toString() ,e);
							failedCount ++;
							continue;
						}
					}
					
					try{
						
						this.processDoc(row, solrCore);
						
						if(sucCount % 10000 == 0){
							logger.warn("[" + coreName + "] Dump的当前成功总条数 ==> " + sucCount + "  失败的总条数  ==> " + failedCount);
						}
					} catch (Exception e){
						logger.error("["+ coreName + "] UpdateHandler处理文档（新增，删除，更新文档） ==> " + row.toString() + "失败",e);
						failedCount ++;
						continue;
					}
					sucCount++;
				}
			}
		} catch(Exception e) {
			logger.error("["+ coreName + "] Dump过程当中发生异常",e);
			isFatal = true;
			throw new DumpFatalException("[" + coreName + "] Dump过程失败", e);
		} finally {
			try {
				this.timer.end();
				logger.warn("["+ coreName + "] 结束此次Dump数据过程，总耗时 ：" + timer.getConsumeTime() + " s  平均耗时: " + timer.getAverageTime() +" s  累计执行次数：" + timer.getTotalTimes() + " 此次Dump成功的数据总量 : " + sucCount +"  失败的总量：" + failedCount);
				this.dataProvider.close();
			} catch (Exception e) {
				logger.error("["+ coreName + "] IndexProvider构建索引完毕，进行后期扫尾工作时出现异常.",e);
			} finally {
				this.isDumping.set(false);
				this.afterDump(isFatal,sucCount,failedCount);
			}
			this.interrupted.set(false);
		}
	}
	
	public void commintWithOptimize() throws Exception {
		logger.warn("提交且优化索引.");
		this.solrCore.getUpdateHandler().commit(SolrCommandBuilder.generateCommitCommand(true));
	}
	
	public void commintWithoutOptimize() throws Exception {
		logger.warn("提交但不优化索引");
		ExecutorService exec = null;
		try {
			logger.warn("commintWithoutOptimize执行时,当前core的路径是：" + this.solrCore.getDataDir());
			//利用反射获取SolrCore的searcherExecutor属性，以防全量完成执行swapCore关闭solrCore之后，增量继续提交
			Field field = this.solrCore.getClass().getDeclaredField("searcherExecutor"); 
			field.setAccessible(true);
			exec = (ExecutorService) field.get(solrCore);
		} catch (Exception e) {
			throw new RuntimeException("提交但不优化索引  --> ;SolrCore对象的searcherExecutor属性失败", e);
		} 
		if (!exec.isShutdown()) {
			this.solrCore.getUpdateHandler().commit(SolrCommandBuilder.generateCommitCommand(false));
		} else {
			throw new RuntimeException("当前路径："  +this.solrCore.getDataDir()+ " 的SolrCore已经关闭，放弃此次增量提交");
		}
		
	}
	
	protected abstract void processDoc(Map<String,String> row,SolrCore solrCore) throws Exception;
	
	protected abstract void beforeDump() throws DumpFatalException;
	
	protected abstract void afterDump(boolean isFatal,int sucCount,int failedCount)throws DumpFatalException;
	
	protected boolean hasNext(){
		try{
			return dataProvider.hasNext();
		} catch (Exception e){
			return  false;
		}
	}

	public DataProvider getDataProvider() {
		return dataProvider;
	}

	public void setDataProvider(DataProvider dataProvider) {
		this.dataProvider = dataProvider;
	}

	public DataProcessor getDataProcessor() {
		return dataProcessor;
	}

	public void setDataProcessor(DataProcessor dataProcessor) {
		this.dataProcessor = dataProcessor;
	}

	public GroupFilter getGroupFilter() {
		return groupFilter;
	}

	public void setGroupFilter(GroupFilter groupFilter) {
		this.groupFilter = groupFilter;
	}

	public SolrCore getSolrCore() {
		return solrCore;
	}

	public void setSolrCore(SolrCore solrCore) {
		this.solrCore = solrCore;
		this.coreName = solrCore.getName();
	}

	public AtomicBoolean getIsDumping() {
		return isDumping;
	}

	public void setIsDumping(AtomicBoolean isDumping) {
		this.isDumping = isDumping;
	}

	public Timer getTimer() {
		return timer;
	}

	public void setTimer(Timer timer) {
		this.timer = timer;
	}

	public AtomicBoolean getInterrupted() {
		return interrupted;
	}

	public void setInterrupted(AtomicBoolean interrupted) {
		this.interrupted = interrupted;
	}
}
