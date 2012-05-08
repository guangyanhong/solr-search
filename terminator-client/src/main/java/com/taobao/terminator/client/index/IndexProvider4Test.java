package com.taobao.terminator.client.index;

import com.taobao.terminator.client.index.transmit.IndexTransmitor;
import com.taobao.terminator.client.index.transmit.IndexTransmitor4Test4LocalFile;
import com.taobao.terminator.common.constant.IndexType;

public class IndexProvider4Test extends TerminatorIndexProvider{
	private boolean localFileTest = false;
	
	public IndexProvider4Test(boolean localFileTest){
		this.localFileTest = localFileTest;
	}
	
	@Override
	protected IndexTransmitor createIndexTransmitor(String serviceName,String groupName, IndexType indexType) {
		return !localFileTest ? 
				new IndexTransmitor4Test(serviceName, groupName, indexType):
				new IndexTransmitor4Test4LocalFile(serviceName, groupName, indexType);
	}
}
