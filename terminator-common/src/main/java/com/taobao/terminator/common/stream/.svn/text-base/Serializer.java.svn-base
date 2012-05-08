package com.taobao.terminator.common.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serializer {

	public static Object bytesToObject(byte[] bytes) {
		if (bytes == null)
			return null;

		ByteArrayInputStream bis = null;
		ObjectInputStream os = null;

		try {
			bis = new ByteArrayInputStream(bytes);
			os = new ObjectInputStream(bis);

			return os.readObject();
		} catch (Exception e) {
			throw new RuntimeException(e);
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
	
	public static void writeObject(DataOutputStream out,Serializable obj) throws IOException{
		byte[] b = objectToBytes(obj);
		out.writeInt(b.length);
		out.write(b);
	}
	
	public static Object readObject(DataInputStream in) throws IOException {
		int size = in.readInt();
		byte[] b = new byte[size];
		in.readFully(b);
		return bytesToObject(b);
	}

	public static byte[] objectToBytes(Object object) {
		ByteArrayOutputStream bos = null;
		ObjectOutputStream os = null;

		try {
			bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			os.writeObject(object);

			return bos.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {

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

}
