package com.taobao.terminator.core.realtime.commitlog2;

import java.io.File;

public class ServerMain {
	public static void main(String[] args) throws Exception {
		File baseDir = new File("C:\\CL");
		CommitLogSyncServer server = new CommitLogSyncServer(baseDir, "localhost", 12345, 2);
		server.start();
	}
}
