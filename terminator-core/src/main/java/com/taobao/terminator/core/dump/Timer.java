package com.taobao.terminator.core.dump;

/**
 *  执行过程 计时器
 * @author yusen
 */
public class Timer{
	public static final int MILLION_SECOND = 1;
	public static final int SECOND = 2;
	public static final int MINUTE = 3;
	
	private int  timeUnit = SECOND;
	private long startTime;
	private long endTime;
	private long cosumeTime ;
	private int  totalTimes;
	private long averageTime;
	private boolean isStarted = false;
	private boolean isEnded   = true;
	
	public Timer(){}
	
	public Timer(int timeUnit){
		this.timeUnit = timeUnit;
	}
	
	public void start(){
		if(!isEnded) return ;
		isStarted = true;
		startTime = System.currentTimeMillis();
	}
	
	public void end(){
		if(!isStarted) return;
		endTime = System.currentTimeMillis();
		cosumeTime = endTime-startTime;
		totalTimes ++;
		averageTime = (averageTime * (totalTimes - 1) + cosumeTime) / totalTimes;
		isEnded = true;
	}
	
	public long getConsumeTime(int timeUnit) {
		switch (timeUnit) {
		case MILLION_SECOND:
			return cosumeTime;
		case SECOND:
			return cosumeTime / 1000;
		case MINUTE:
			return cosumeTime / 1000 / 60;
		default:
			return cosumeTime;
		}
	}
	
	public long getConsumeTime(){
		return this.getConsumeTime(timeUnit);
	}
	
	public long getAverageTime(int timeUnit){
		switch (timeUnit) {
		case MILLION_SECOND:
			return averageTime;
		case SECOND:
			return averageTime / 1000;
		case MINUTE:
			return averageTime / 1000 / 60;
		default:
			return averageTime;
		}
	}
	
	public long getAverageTime(){
		return this.getAverageTime(timeUnit);
	}
	
	public void clear(){
		totalTimes = 0;
		averageTime = 0;
	}
	
	public int getTotalTimes(){
		return totalTimes;
	}
}
