package com.taobao.terminator.common.stream;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class Streamer {

	private static Logger logger = Logger.getLogger(Streamer.class);
	public static final int BUFFER_SIZE = 1024 * 1024 * 8;

	public static void close(Closeable closeable) {
		if (closeable != null)
			try {
				closeable.close();
			} catch (IOException e) {
				logger.warn("Error when close Closeable", e);
			}
	}

	public static ByteBuffer constructStream(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
		buffer.putInt(bytes.length);
		buffer.put(bytes);
		buffer.flip();
		return buffer;
	}

	public static byte[] readStream(DataInputStream input) throws IOException {
		int length = input.readInt();
		byte[] contentBytes = new byte[length];
		input.readFully(contentBytes);
		return contentBytes;
	}
}
