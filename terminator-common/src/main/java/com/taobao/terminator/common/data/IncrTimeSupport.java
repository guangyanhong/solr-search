package com.taobao.terminator.common.data;

import java.util.Date;

/**
 * ������startTime��endTime������ͳһ����Ӧ����Ҫ�Լ���չDataProvider��ʱ��<br>
 * �û���ͨ��ʵ�ִ˽ӿڵķ�ʽ����ȡ������ʱ�䣬���һ������ʵ��DataProvider��ʱ���û��������й�����������ֹʱ�䣬
 * �������û��Ŀ����ɱ�
 * 
 * @author yusen
 *
 */
public interface IncrTimeSupport {
	public void setIncrTime(Date startTime,Date endTime);
}
