package com.taobao.terminator.common.data.processor;

import java.util.Map;

/**
 * DataProvider获取的数据进行处理的接口
 * 
 * @author yusen
 */
public interface DataProcessor {
	
	/**
	 * 该DataProcessor的职责描述
	 * @return
	 */
	String getDesc();
	
	/**
	 * 数据处理方法,如果该行数据不符合要求，亦即不需要索引的数据,直接返回null，dump程序会自动忽略该数据
	 * 
	 * @param map 代表一行数据，亦即一个Document数据源
	 * @throws DataProcessException
	 */
	ResultCode process(Map<String,String> map) throws DataProcessException;
	
	public class ResultCode{
		int code;
		String msg;
		
		public static final ResultCode SUC = new ResultCode(0,"处理成功");
		public static final ResultCode FAI = new ResultCode(-1, "处理失败");

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
