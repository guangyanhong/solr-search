package com.taobao.terminator.common.data.processor;

import java.util.Map;

/**
 * DataProvider��ȡ�����ݽ��д���Ľӿ�
 * 
 * @author yusen
 */
public interface DataProcessor {
	
	/**
	 * ��DataProcessor��ְ������
	 * @return
	 */
	String getDesc();
	
	/**
	 * ���ݴ�����,����������ݲ�����Ҫ���༴����Ҫ����������,ֱ�ӷ���null��dump������Զ����Ը�����
	 * 
	 * @param map ����һ�����ݣ��༴һ��Document����Դ
	 * @throws DataProcessException
	 */
	ResultCode process(Map<String,String> map) throws DataProcessException;
	
	public class ResultCode{
		int code;
		String msg;
		
		public static final ResultCode SUC = new ResultCode(0,"����ɹ�");
		public static final ResultCode FAI = new ResultCode(-1, "����ʧ��");

		private ResultCode(int code, String msg) {
			super();
			this.code = code;
			this.msg = msg;
		}
		
		public ResultCode(String msg){
			this(-2,msg);
		}
		
		public boolean isSuc(){
			return this.code == 0;
		}

		@Override
		public String toString() {
			return "code : " + code + "  msg : " + msg;
		}
	}
}
