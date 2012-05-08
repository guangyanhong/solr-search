package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;
import java.io.IOException;

public class MainTest {
	
	public static void main(String args[]) throws IOException{
		File baseDir = new File("C:\\CL");
		
		CommitLogAccessor ac = new CommitLogAccessor(baseDir, 20 * 1024*1024, null, 1,true);
		
		for (int i = 0 ;i < 10000000; i++) {
			ac.write("Hello" + i);
		}
	}
}
