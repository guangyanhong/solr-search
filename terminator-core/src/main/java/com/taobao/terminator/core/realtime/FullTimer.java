package com.taobao.terminator.core.realtime;

/**
 * 决定全量构建之后IndexBuilderJob的CommigLogReader应该回退到Checkpoint中的那个时间点开始补偿全量期间的实时数据<br>
 * 叽歪：TMD,这个类名太难想了，想了很长时间搞了这么一个不让人理解的名字，先凑活着吧，灵感来了再该！！！
 * @author yusen
 *
 */
public interface FullTimer {
	/**
	 * 获取全量开始的时间点
	 * 
	 * @return
	 */
	public long getTime();
}