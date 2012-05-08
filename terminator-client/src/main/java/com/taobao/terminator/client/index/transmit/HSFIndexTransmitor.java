package com.taobao.terminator.client.index.transmit;

import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorHSFContainer;
import com.taobao.terminator.common.constant.IndexType;
import com.taobao.terminator.common.protocol.MasterService;

public class HSFIndexTransmitor implements IndexTransmitor{
	private MasterService indexWriteService = null;
	private IndexType indexType = null;
	
	public HSFIndexTransmitor(String serviceName,String groupName,IndexType indexType){
		this(serviceName + "-"  + groupName,indexType);
	}
	
	public HSFIndexTransmitor(String coreName,IndexType indexType){
		//TODO:this.indexWriteService = TerminatorHSFContainer.getMasterService(coreName);
		this.indexType = indexType;
	}
	
	@Override
	public boolean start() throws IndexTransmitException {
		try {
			if(IndexType.FULL.equals(indexType)){
				return indexWriteService.startFullDump(TerminatorCommonUtils.getLocalHostIP());
			}else if(IndexType.INCREMENT.equals(indexType)){
				return indexWriteService.startIncDump();
			}
		} catch(Exception e){
			throw new IndexTransmitException("����Start�����쳣",e);
		}
		return false;
	}

	@Override
	public boolean transmit(byte[] data) throws IndexTransmitException {
		try {
			if(IndexType.FULL.equals(indexType)){
				return indexWriteService.fullDump(TerminatorCommonUtils.getLocalHostIP(),data);
			}else if(IndexType.INCREMENT.equals(indexType)){
				return indexWriteService.incrDump(data);
			}
			return false;
		} catch(Exception e){
			throw new IndexTransmitException("����dump�����쳣",e);
		}
	}
	
	@Override
	public boolean finish() throws IndexTransmitException {
		try{
			if(IndexType.FULL.equals(indexType)){
				return indexWriteService.finishFullDump(TerminatorCommonUtils.getLocalHostIP());
			}else if(IndexType.INCREMENT.equals(indexType)){
				return indexWriteService.finishIncrDump();
			}
		}catch(Exception e){
			throw new IndexTransmitException("����finish�����쳣",e);
		}
		return false;
	}
}