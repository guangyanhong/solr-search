package com.taobao.terminator.core;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.xml.sax.SAXException;

import com.taobao.terminator.core.index.FullIndexFileSearcher;
import com.taobao.terminator.core.index.FullIndexWriteJob;
import com.taobao.terminator.core.index.IndexFileSearcher;

public class SolrDataImporterTest {	
	//测试在有一个solr的情况之下，新建一个solr core，并用新的solr进行索引的导入，然后交换
	public static void main(String[] args) throws IOException, 
	ParserConfigurationException, SAXException, SchedulerException, ParseException{
		CoreContainer cores;
		//SolrXMLDataImporter dataImporter;
		System.setProperty("solr.solr.home", "E:\\taobao_workspace\\solr_server\\");
		CoreContainer.Initializer init = new CoreContainer.Initializer();
		cores = init.initialize();
		
		SolrCore orignCore = cores.getCore("testCore");
		
		//FullIndexWriteJob job = new FullIndexWriteJob();
		IndexFileSearcher fileSearcher = new FullIndexFileSearcher(new File("E:\\taobao_workspace\\output"));
		SchedulerFactory schedFact = new StdSchedulerFactory();
		Scheduler sched = schedFact.getScheduler();
		
		SchedulerContext context = sched.getContext();
		context.put("solrCore", orignCore);
		context.put("fullIndexFileSearcher", fileSearcher);
		
		JobDetail jobDetail = new JobDetail("fullIndexing", "0", FullIndexWriteJob.class);
		jobDetail.setDurability(true);
		//SimpleTrigger trigger = new SimpleTrigger("jobTrigger", "0", 1, Long.MAX_VALUE);
		
		//sched.scheduleJob(jobDetail, trigger);
		sched.addJob(jobDetail, false);
		sched.triggerJob("fullIndexing", "0");
	}
}
