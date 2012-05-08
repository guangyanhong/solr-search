package com.taobao.terminator.core.realtime.commitlog2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 序列化的方式
 * 
 * @author yusen
 *
 */
public interface Serializer {
	
	public byte[] toByte(Object obj) throws IOException;
	
	public Object toObject(byte[] b) throws IOException,ClassNotFoundException;
	
	public static class DefaultSerializer implements Serializer{

		@Override
		public byte[] toByte(Object obj) throws IOException{
			ObjectOutputStream objOut = null;
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				objOut = new ObjectOutputStream(out);
				objOut.writeObject(obj);
				return out.toByteArray();
			} finally {
				if(objOut != null) {
					objOut.close();
				}
			}
		}

		@Override
		public Object toObject(byte[] b) throws IOException, ClassNotFoundException{
			ObjectInputStream objIn = null;
			try {
				ByteArrayInputStream in = new ByteArrayInputStream(b);
				objIn = new ObjectInputStream(in);
				return objIn.readObject();
			} finally {
				if(objIn != null) {
					objIn.close();
				}
			}
		}
	}
}
