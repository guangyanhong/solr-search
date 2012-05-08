package com.taobao.terminator.core.realtime.commitlog2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SyncUtils {
	
	/**
	 * 表示网络出问题的异常，区别于别的IO异常，比如文件IO异常等
	 * 
	 * @author yusen
	 */
	public static class NetException extends IOException {
		private static final long serialVersionUID = 1L;

		public NetException() {
			super();
		}

		public NetException(String message, Throwable cause) {
			super(message, cause);
		}

		public NetException(String message) {
			super(message);
		}

		public NetException(Throwable cause) {
			super(cause);
		}
	}
	
	public static void writeObject(DataOutputStream out, Object obj) throws IOException {
		try {
			byte[] b = objectToBytes(obj);
			out.writeInt(b.length);
			out.write(b);
		} catch (IOException e) {
			throw new NetException(e);
		}
	}
	
	public static Object readObject(DataInputStream in) throws IOException, ClassNotFoundException{
		try {
			int size = in.readInt();
			byte[] b = new byte[size];
			in.readFully(b);
			return bytesToObject(b);
		} catch (IOException e) {
			throw new NetException(e);
		}
	}
	
	public static byte[] objectToBytes(Object object) throws IOException {
		ByteArrayOutputStream bos = null;
		ObjectOutputStream os = null;

		try {
			bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			os.writeObject(object);

			return bos.toByteArray();
		}  finally {
			try {
				if (os != null)
					os.close();
			} catch (Exception e) {
			}
			try {
				if (bos != null)
					bos.close();
			} catch (Exception e) {
			}
		}
	}
	
	public static Object bytesToObject(byte[] bytes) throws IOException, ClassNotFoundException {
		if (bytes == null)
			return null;

		ByteArrayInputStream bis = null;
		ObjectInputStream os = null;

		try {
			bis = new ByteArrayInputStream(bytes);
			os = new ObjectInputStream(bis);
			return os.readObject();
		} finally {

			try {
				if (os != null)
					os.close();
			} catch (Exception e) {
			}
			try {
				if (bis != null)
					bis.close();
			} catch (Exception e) {
			}
		}
	}
}
