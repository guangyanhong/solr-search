package com.taobao.terminator.common.timers;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

import com.taobao.terminator.common.timers.Utils.Dhms;

/**
 * {permonth 2 12:00:00} 每个月的第二天的12点
 * 
 * @author yusen
 */
public class PermonthExpression implements TimerExpression {

	@Override
	public TimerInfo parse(String expression) throws TimerExpressionException {
		Dhms dhms = Utils.parseToDhms(expression);

		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.DAY_OF_MONTH, dhms.day);
		cal.set(Calendar.HOUR_OF_DAY, dhms.hr);
		cal.set(Calendar.MINUTE, dhms.min);
		cal.set(Calendar.SECOND, dhms.sec);

		Calendar nCal = new GregorianCalendar();
		long nowDay = nCal.get(Calendar.DAY_OF_MONTH);
		long settingDay =cal.get(Calendar.DAY_OF_MONTH);
			
		if(System.currentTimeMillis() > cal.getTimeInMillis()){
			long intervalDay = (settingDay +30 -nowDay);
			cal.add(Calendar.DAY_OF_MONTH, (int) intervalDay);
		}
		
		long intervalTime = cal.getTimeInMillis() - System.currentTimeMillis();
		
		TimerInfo ti = new TimerInfo(intervalTime/1000, 30 * 24 * 60 * 60);
		ti.timeUnit = TimeUnit.SECONDS;
		return ti;
	}
}
