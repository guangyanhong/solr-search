package com.taobao.terminator.common.data.timer;

import java.util.Date;

/**
 * 增量时间管理
 * 
 * @author yusen
 */
public interface TimerManager {
	/**
	 * 获取增量的时间范围区间,并作一些时间的存储 初始化的工作
	 * 
	 * @return
	 * @throws TimeManageException
	 */
	public StartAndEndTime initTimes() throws TimeManageException;
	
	/**
	 * 单纯的获取开始结束时间 不做其他的操作
	 * 
	 * @return
	 */
	public StartAndEndTime justGetTimes() throws TimeManageException ;
	
	/**
	 * 重置增量时间点
	 */
	public StartAndEndTime resetTimes() throws TimeManageException;
	
	/**
	 * 增量的起始结束时间
	 *  
	 * @author yusen
	 */
	public class StartAndEndTime{
		public StartAndEndTime(Date startTime,Date endTime){
			this.endTime = endTime;
			this.startTime = startTime;
		}
		public Date endTime ;
		public Date startTime ;
	}
}
