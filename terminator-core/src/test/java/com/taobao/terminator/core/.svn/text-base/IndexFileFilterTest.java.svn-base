package com.taobao.terminator.core;

import java.io.File;
import java.util.Collection;

import org.junit.Test;

import com.taobao.terminator.core.util.FullIndexFileFilter;
import com.taobao.terminator.core.util.IndexFileUtils;

public class IndexFileFilterTest {
	@Test
	public void test(){
		File indexDir = new File("E:\\taobao_workspace\\output");
		FullIndexFileFilter filter = new FullIndexFileFilter(4, null);
		
		Collection<File> files = IndexFileUtils.listFile(indexDir, filter, false);
		
		for(File file : files){
			System.out.println(file.getName());
		}
	}
}
