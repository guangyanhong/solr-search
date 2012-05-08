package com.taobao.terminator.core.dump;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.core.SolrCore;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

/**
 * ����Dump����
 * 
 * @author yusen
 */
public class IncrDumpJob implements InterruptableJob {
	
	private static Log logger = LogFactory.getLog(IncrDumpJob.class);
	
	private IncrIndexProvider incrIndexProvider;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		logger.warn("**********����ʱ������ʼ**********");
		incrIndexProvider = null;
		SolrCore solrCore = null;
		try {
			incrIndexProvider = (IncrIndexProvider)context.getScheduler().getContext().get(DumpService.JOB_INCR_INDEX_PROVIDER);
			solrCore = (SolrCore)context.getScheduler().getContext().get(DumpService.JOB_SOLR_CORE);
		} catch(Exception e) {
			throw new JobExecutionException("[Incr-Index] ��ScheduleContext�л�ȡ����ʧ��",e);
		}
		
		if(incrIndexProvider == null || solrCore == null) {
			throw new JobExecutionException("[Incr-Index] ���IndexProvider��SolrCoreʧ��");
		}
		
		try {
			incrIndexProvider.setSolrCore(solrCore);
			incrIndexProvider.dump();
		} catch(DumpFatalException dfe) {
			logger.fatal("[Incr-Index] dump����ʱ�����������󣬱�������dumpʧ��", dfe);
			throw new JobExecutionException("[Incr-Index] dump����ʱ�����������󣬱�������dumpʧ��", dfe);
		} 
		
		logger.warn("[Incr-Index] ��������˳��������" + " ��������ʱ�䣺" + new Date());
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		this.incrIndexProvider.getInterrupted().set(true);
	}
}
