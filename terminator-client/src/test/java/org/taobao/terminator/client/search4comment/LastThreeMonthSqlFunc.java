package org.taobao.terminator.client.search4comment;

import java.util.Calendar;
import java.util.Date;

import com.taobao.terminator.client.index.data.SqlFunction;

public class LastThreeMonthSqlFunc implements SqlFunction {

	@Override
	public String getPlaceHolderName() {
		return "lastThreeMonth";
	}

	@Override
	public String getValue() {
		Date date = new Date();
	      Calendar calendar = Calendar.getInstance();
	      calendar.setTime(date);
	      calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) - 2);
	     /* return CalendarUtil.toString(CalendarUtil.zerolizedTime(calendar
	            .getTime()), CalendarUtil.TIME_PATTERN);*/
	      return null;

	}

}
