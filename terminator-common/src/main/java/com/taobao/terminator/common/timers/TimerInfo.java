package com.taobao.terminator.common.timers;

import java.util.concurrent.TimeUnit;

public class TimerInfo {
	public long initDelay;
	public long period;
	public TimeUnit timeUnit = TimeUnit.MILLISECONDS;
	
	public TimerInfo(long initDelay,long period) {
		this.initDelay = initDelay;
		this.period = period;
	}

	@Override
	public String toString() {
		return "TimerInfo [initDelay=" + initDelay + ", period=" + period + "]";
	}
}