package com.taobao.terminator.common.data;

import java.util.Date;

/**
 * 增量的startTime和endTime有终搜统一管理，应用需要自己扩展DataProvider的时候，<br>
 * 用户可通过实现此接口的方式来获取这两个时间，如此一来自行实现DataProvider的时候用户无需自行管理增量的起止时间，
 * 降低了用户的开发成本
 * 
 * @author yusen
 *
 */
public interface IncrTimeSupport {
	public void setIncrTime(Date startTime,Date endTime);
}
