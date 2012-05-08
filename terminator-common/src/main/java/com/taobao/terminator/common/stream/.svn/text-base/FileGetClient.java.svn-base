package com.taobao.terminator.common.stream;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class FileGetClient {
	
	private final int port;
	private final String host;

	public FileGetClient(String host, int port) {
		this.port = port;
		this.host = host;
	}

	public int doGetFile(String type, String name, FileOutputStream out) throws IOException {
		SocketChannel channel = SocketChannel.open();
		FileGetRequest fileGetReq = new FileGetRequest(type, name);
		try {
			channel.socket().bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
			channel.connect(new InetSocketAddress(host, port));

			byte[] bytes = Serializer.objectToBytes(fileGetReq);
			ByteBuffer buffer = Streamer.constructStream(bytes);
			channel.write(buffer);

			assert buffer.remaining() == 0;

			DataInputStream input = new DataInputStream(channel.socket().getInputStream());
			byte[] contentBytes = Streamer.readStream(input);
			FileGetResponse fileGetRes = (FileGetResponse) Serializer.bytesToObject(contentBytes);

			int code = fileGetRes.getCode();

			if (code != FileGetResponse.SUCCESS) {
				return code;
			}

			long total  = fileGetRes.getLength();
			byte data[] = new byte[Streamer.BUFFER_SIZE];
			long readed = 0;
			int readnum = 0;
			while ((readnum = input.read(data)) != -1) {
				out.write(data, 0, readnum);
				readed += readnum;
			}
			
			if (readed != total) return FileGetResponse.FILE_EXCEPTION;
			
			return FileGetResponse.SUCCESS;

		} finally {
			Streamer.close(channel);
		}
	}
}
