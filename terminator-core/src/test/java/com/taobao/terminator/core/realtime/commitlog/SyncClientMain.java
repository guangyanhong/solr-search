package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.io.IOException;



public class SyncClientMain {
	public static void main(String[] args) throws IOException {
		CommitLogSyncClient client =new CommitLogSyncClient("localhost", 12345, new File("C:\\commitLog2"));
		client.doSync();
	}
}
