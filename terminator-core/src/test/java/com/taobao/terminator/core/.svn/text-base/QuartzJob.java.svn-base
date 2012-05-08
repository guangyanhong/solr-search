package com.taobao.terminator.core;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.SchedulerListener;
import org.quartz.StatefulJob;
import org.quartz.Trigger;

public class QuartzJob implements Job{
		public void execute(JobExecutionContext arg0)
				throws JobExecutionException {
			System.out.println("job executed.");
			String str = "hello world!";
			
			arg0.put("str", str);
			/*try {
				System.out.println("Sleeping..");
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("wake up...");*/
			/*while(true){
				
			}*/
		}
}
