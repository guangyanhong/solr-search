package com.taobao.terminator.core.realtime;

/**
 * ����ȫ������֮��IndexBuilderJob��CommigLogReaderӦ�û��˵�Checkpoint�е��Ǹ�ʱ��㿪ʼ����ȫ���ڼ��ʵʱ����<br>
 * ߴ�᣺TMD,�������̫�����ˣ����˺ܳ�ʱ�������ôһ���������������֣��ȴջ��Űɣ���������ٸã�����
 * @author yusen
 *
 */
public interface FullTimer {
	/**
	 * ��ȡȫ����ʼ��ʱ���
	 * 
	 * @return
	 */
	public long getTime();
}