package com.taobao.terminator.common.data;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ���ݻ�ȡ�Ľӿ�
 * 
 * @author yusen
 *
 */
public interface DataProvider {
	Log logger = LogFactory.getLog(DataProvider.class);

	/**
	 * ��ʼ������������ִ��ִ��SQL�Ȳ���
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception;

	/**
	 * �ж��Ƿ�������
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean hasNext() throws Exception;

	/**
	 * ��ȡһ����¼
	 * 
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> next() throws Exception;

	/**
	 * �ͷ���Դ���������ݿ����ӣ��ļ������
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception;
}
