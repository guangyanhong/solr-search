package com.taobao.terminator.common.times;

import org.junit.Test;

import com.taobao.terminator.common.timers.TimerExpressions;

public class TimerTest {
	private TimerExpressions tes = new TimerExpressions();
	
	@Test
	public void testPerMonth() throws Exception {
		System.out.println(tes.parse("{permonth 28 12:00:00}"));
	}
	
	@Test
	public void testPerWeek() throws Exception {
		System.out.println(tes.parse("{perweek 2 12:00:00}"));
	}
	
	public void testPerDay() throws Exception {
		System.out.println(tes.parse("{perday 18:51:00}"));
	}
}
