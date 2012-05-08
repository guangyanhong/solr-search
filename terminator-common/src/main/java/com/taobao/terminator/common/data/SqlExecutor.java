package com.taobao.terminator.common.data;

import java.util.List;
import java.util.Map;

/**
 * Sqlִ����
 */
public interface SqlExecutor {
	/**
	 * ��ʼ���������ٴοɻ�ȡ���ӣ���SQLִ��ǰ��׼������
	 * @throws Exception
	 */
	public void init() throws Exception;
	
	/**
	 * ִ��SQL
	 * 
	 * @param param  ��SQL����ռλ������Ĳ���
	 * @return 
	 * @throws Exception
	 */
	public List<Map<String, String>> execute(Map<String, String> param) throws Exception;
	
	/**
	 * �ͷ����ݿ����ӵ���Դ
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception;
}
