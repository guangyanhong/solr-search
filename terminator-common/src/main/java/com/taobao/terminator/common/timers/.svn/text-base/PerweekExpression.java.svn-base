package com.taobao.terminator.common.timers;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.taobao.terminator.common.timers.Utils.Dhms;

/**
 * {perweek 2 12:00:00} 每周2的12点
 *
 * @author yusen
 *
 */
public class PerweekExpression implements TimerExpression {

	@Override
	public TimerInfo parse(String expression) throws TimerExpressionException {
		Dhms dhms = Utils.parseToDhms(expression);

		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.DAY_OF_WEEK, dhms.day);
		cal.set(Calendar.HOUR_OF_DAY, dhms.hr);
		cal.set(Calendar.MINUTE, dhms.min);
		cal.set(Calendar.SECOND, dhms.sec);

		long settingDay = cal.get(Calendar.DAY_OF_WEEK);

		// 当前日期
		Calendar nowCal = new GregorianCalendar();
		long nowDay = nowCal.get(Calendar.DAY_OF_WEEK);

		if (System.currentTimeMillis() > cal.getTimeInMillis()) {
			long intervalDay = settingDay + 7 - nowDay;
			cal.add(Calendar.DAY_OF_WEEK_IN_MONTH, (int) intervalDay);
		}

		long intervalTime = cal.getTimeInMillis() - System.currentTimeMillis();

		return new TimerInfo(intervalTime, 7 * 24 * 60 * 60 * 1000);
	}
}
