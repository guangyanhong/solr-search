/*package com.taobao.terminator.core;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.xml.sax.SAXException;

import com.taobao.terminator.core.index.FullIndexFileSearcher;
import com.taobao.terminator.core.index.FullIndexWriteJob;
import com.taobao.terminator.core.index.IncrIndexFileSearcher;
import com.taobao.terminator.core.index.IncrIndexWriteJob;
import com.taobao.terminator.core.index.IndexContext;
import com.taobao.terminator.core.index.IndexFileSearcher;
import com.taobao.terminator.core.util.IndexUtils;

public class SolrXmlDataImporterTest implements JobListener{
	private SolrCore solrCore;
	private Scheduler scheduler;
	
	@Test
	public void test() throws SchedulerException, IOException, ParserConfigurationException, SAXException{
		CoreContainer cores;
		System.setProperty("solr.solr.home", "E:\\taobao_workspace\\solr_server\\");
		CoreContainer.Initializer init = new CoreContainer.Initializer();
		cores = init.initialize();
		
		this.solrCore = cores.getCore("testCore");
		
		//FullIndexWriteJob job = new FullIndexWriteJob();
		IndexFileSearcher fullfileSearcher = new FullIndexFileSearcher(new File("E:\\taobao_workspace\\output\\full"));
		IncrIndexFileSearcher incrfileSearcher = new IncrIndexFileSearcher(new File("E:\\taobao_workspace\\output\\incr"));
		
		SchedulerFactory schedulerFactory = new StdSchedulerFactory();
		this.scheduler = schedulerFactory.getScheduler();
		
		JobDetail jobDetail = new JobDetail("fullIndexWrite", "index", FullIndexWriteJob.class);
		jobDetail.setDurability(true);
		this.scheduler.addJob(jobDetail, false);
	
		SchedulerContext context = this.scheduler.getContext();
		context.put("solrCore", this.solrCore);
		context.put("fullIndexFileSearcher", fullfileSearcher);
		context.put("incrIndexFileSearcher", incrfileSearcher);
		
		this.scheduler.addJobListener(this);
		jobDetail.addJobListener("test");
		
		this.scheduler.start();
		
		this.scheduler.triggerJob("fullIndexWrite", "index");
		
		this.scheduler.triggerJob("fullIndexWrite", "index");
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//IndexContext.isDataTransmitFinish.set(true);
		while(true){
			
		}
	}

	public String getName() {
		return "test";
	}

	public void jobExecutionVetoed(JobExecutionContext arg0) {
		
	}

	public void jobToBeExecuted(JobExecutionContext arg0) {
		
	}

	public void jobWasExecuted(JobExecutionContext arg0,
			JobExecutionException arg1) {
		SolrCore newCore = (SolrCore)arg0.get("newSolrCore");
		this.solrCore = IndexUtils.swapCores(this.solrCore,newCore);
		try {
			this.scheduler.getContext().remove("solrCore");
			this.scheduler.getContext().put("solrCore", this.solrCore);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}
}
*/