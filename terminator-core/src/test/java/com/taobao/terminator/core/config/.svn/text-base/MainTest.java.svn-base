package com.taobao.terminator.core.config;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.log4j.DailyRollingFileAppender;

public class MainTest {
	
	  static final int TOP_OF_TROUBLE=-1;
	  static final int TOP_OF_MINUTE = 0;
	  static final int TOP_OF_HOUR   = 1;
	  static final int HALF_DAY      = 2;
	  static final int TOP_OF_DAY    = 3;
	  static final int TOP_OF_WEEK   = 4;
	  static final int TOP_OF_MONTH  = 5;
	
	public static void main(String[] args) {
		RollingCalendar c = new RollingCalendar();
		c.setType(2);
		System.out.println(c.getNextCheckDate(new Date()));
		
		System.out.println(System.currentTimeMillis());
	}
	
	static class RollingCalendar extends GregorianCalendar {
		  private static final long serialVersionUID = -3560331770601814177L;

		  int type = TOP_OF_TROUBLE;

		  public RollingCalendar() {
		    super();
		  }  

		 public RollingCalendar(TimeZone tz, Locale locale) {
		    super(tz, locale);
		  }  

		  void setType(int type) {
		    this.type = type;
		  }

		  public long getNextCheckMillis(Date now) {
		    return getNextCheckDate(now).getTime();
		  }

		  public Date getNextCheckDate(Date now) {
		    this.setTime(now);

		    switch(type) {
		    case TOP_OF_MINUTE:
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			this.add(Calendar.MINUTE, 1);
			break;
		    case TOP_OF_HOUR:
			this.set(Calendar.MINUTE, 0);
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			this.add(Calendar.HOUR_OF_DAY, 1);
			break;
		    case HALF_DAY:
			this.set(Calendar.MINUTE, 0);
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			int hour = get(Calendar.HOUR_OF_DAY);
			if(hour < 12) {
			  this.set(Calendar.HOUR_OF_DAY, 12);
			} else {
			  this.set(Calendar.HOUR_OF_DAY, 0);
			  this.add(Calendar.DAY_OF_MONTH, 1);
			}
			break;
		    case TOP_OF_DAY:
			this.set(Calendar.HOUR_OF_DAY, 0);
			this.set(Calendar.MINUTE, 0);
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			this.add(Calendar.DATE, 1);
			break;
		    case TOP_OF_WEEK:
			this.set(Calendar.DAY_OF_WEEK, getFirstDayOfWeek());
			this.set(Calendar.HOUR_OF_DAY, 0);
			this.set(Calendar.MINUTE, 0);
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			this.add(Calendar.WEEK_OF_YEAR, 1);
			break;
		    case TOP_OF_MONTH:
			this.set(Calendar.DATE, 1);
			this.set(Calendar.HOUR_OF_DAY, 0);
			this.set(Calendar.MINUTE, 0);
			this.set(Calendar.SECOND, 0);
			this.set(Calendar.MILLISECOND, 0);
			this.add(Calendar.MONTH, 1);
			break;
		    default:
			throw new IllegalStateException("Unknown periodicity type.");
		    }
		    return getTime();
		  }
		}
}
