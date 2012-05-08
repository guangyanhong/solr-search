package com.taobao.terminator.core.realtime.commitlog;

import java.io.File;
import java.io.IOException;



public class SyncServerMain {
	public static void main(String[] args) throws IOException {
		CommitLogSyncServer server = new CommitLogSyncServer("localhost", 12345, new CommitLog(new File("C:\\commitLog"),1));
		server.start();
	}
}
