package com.taobao.terminator.core;

import java.text.ParseException;

import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerListener;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;

public class QuartzTest implements JobListener{
	@Test
	public void test() throws SchedulerException,
			InterruptedException, ParseException {
		
		DirectSchedulerFactory schedulerFactory = DirectSchedulerFactory.getInstance();
		RAMJobStore jobStore = new RAMJobStore();
		SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
		jobStore.setMisfireThreshold(60000);
		schedulerFactory.createScheduler("testScheduler", 
				"testScheulerInstance", threadPool, jobStore);
		threadPool.initialize();
		
		Scheduler scheduler = schedulerFactory.getScheduler("testScheduler");
		
		JobDetail job = new JobDetail("job1", "0", QuartzJob.class);
		job.setDurability(true);
		
		//JobDetail job = new JobDetail("job1", "0", QuartzJob.class);
		//CronTrigger incrJobTrigger = new CronTrigger("trigger", "trigger", "0/1 * * * * ?");
		//SimpleTrigger trigger = new SimpleTrigger("trigger", "trigger");
		//scheduler.addSchedulerListener(this);
		
		
		scheduler.addJobListener(this);
		job.addJobListener("myJobListener");
		scheduler.addJob(job, false);
		scheduler.start();
		scheduler.triggerJob("job1", "0");
		/*scheduler.triggerJob("job1", "0");
		scheduler.triggerJob("job1", "0");
		
		scheduler.triggerJob("job1", "0");*/
		
		//scheduler.scheduleJob(job, incrJobTrigger);
		while(true){
			
		}
	}

	public String getName() {
		return "myJobListener";
	}

	public void jobExecutionVetoed(JobExecutionContext arg0) {
		System.out.println("jobExecutionVetoed");
	}

	public void jobToBeExecuted(JobExecutionContext arg0) {
		System.out.println("jobToBeExecuted");
	}

	public void jobWasExecuted(JobExecutionContext arg0,
			JobExecutionException arg1) {
		System.out.println(arg0.getJobDetail().getFullName());	
		System.out.println(arg0.get("str"));
	}
}
