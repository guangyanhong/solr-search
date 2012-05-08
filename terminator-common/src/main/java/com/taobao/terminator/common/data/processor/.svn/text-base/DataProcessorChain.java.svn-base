package com.taobao.terminator.common.data.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据处理链
 * 
 * @author yusen
 *
 */
public class DataProcessorChain implements DataProcessor{
	private List<DataProcessor> dataProcessors = null;
	
	public DataProcessorChain(){
		dataProcessors = new ArrayList<DataProcessor>();
	}

	@Override
	public ResultCode process(Map<String, String> map) throws DataProcessException{
		for(DataProcessor processor : dataProcessors){
			ResultCode rs = processor.process(map);
			if(!rs.isSuc()){
				return rs;
			}
		}
		return ResultCode.SUC;
	}

	public DataProcessorChain addDataProcessors(DataProcessor dataProcessor){
		dataProcessors.add(dataProcessor);
		return this;
	}
	
	public List<DataProcessor> getDataProcessors() {
		return dataProcessors;
	}

	public void setDataProcessors(List<DataProcessor> dataProcessors) {
		this.dataProcessors = dataProcessors;
	}

	@Override
	public String getDesc() {
		String s = null;
		if(dataProcessors != null){
			StringBuilder sb = new StringBuilder();
			int i = 1;
			for(DataProcessor dp : dataProcessors){
				sb.append(i++).append(". ").append(dp.getDesc()).append("\n");
			}
			s = sb.toString();
		}
		return "Processor链，串联所有DataProcessor ==> \n" + (s == null ? "" : s);
	}
}
