package com.taobao.terminator.common.timers;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.taobao.terminator.common.timers.Utils.Hms;

/**
 * {perday 12:00:00} 每天的12点
 * 
 * @author yusen
 *
 */
public class PerdayExpression implements TimerExpression {

	@Override
	public TimerInfo parse(String expression) throws TimerExpressionException {
		Hms hms = Utils.parseToHMS(expression);

		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.HOUR_OF_DAY, hms.hr);
		cal.set(Calendar.MINUTE, hms.min);
		cal.set(Calendar.SECOND, hms.sec);

		if (System.currentTimeMillis() > cal.getTimeInMillis()) {
			cal.add(Calendar.DAY_OF_MONTH, 1);
		}

		return new TimerInfo(cal.getTimeInMillis() - System.currentTimeMillis(), 24 * 60 * 60 * 1000);
	}
}
