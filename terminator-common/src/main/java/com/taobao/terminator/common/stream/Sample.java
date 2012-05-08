package com.taobao.terminator.common.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class Sample {
	
	public static void main(String[] args) throws IOException {
		FileGetServer server = new FileGetServer("127.0.0.1", 8999);
		server.register("index", new FileProvider() {
			@Override
			public File getTargetFile(String name) {
				return new File("D:\\aa\\server", name);
			}
		});
		server.start();

		FileGetClient client = new FileGetClient("127.0.0.1", 8999);

		File out = new File("D:\\aa\\client", "wdx.in.channel");
		FileOutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(out);
			if (client.doGetFile("index", "wdx.in.channel", outputStream) != FileGetResponse.SUCCESS) {
				//TODO
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		server.end();
	}
}
