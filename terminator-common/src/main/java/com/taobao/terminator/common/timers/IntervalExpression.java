package com.taobao.terminator.common.timers;

import com.taobao.terminator.common.timers.Utils.Hms;

/**
 * {interval 00:05:00} √øº‰∏Ù5∑÷÷”
 * 
 * @author yusen
 *
 */
public class IntervalExpression implements TimerExpression{
	@Override
	public TimerInfo parse(String expression) throws TimerExpressionException {
		Hms hms = Utils.parseToHMS(expression);
		int result = 0;

		result += hms.sec;
		result += (60 * hms.min);
		result += (60 * 60 * hms.hr);
		result *= 1000;

		long initDelay = result - (System.currentTimeMillis() % result);
		return new TimerInfo(initDelay, result);
	}

}
