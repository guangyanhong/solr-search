package org.taobao.terminator.client;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.Deflater;


public class MainTest {
	public static final int LOWER = 0x01;
	public static final int UPPER = 0x02;
	public static final int DIGIT = 0x04;
	public static final int SUBWORD_DELIM = 0x08;
	static byte[] defaultWordDelimTable;

	public static void mainss(String[] args) throws UnsupportedEncodingException {
		//System.out.println(SortableStr2long("12fdas3", 0, 5));
		String email = "浙江省杭州市华星路创业大厦6楼小邮局";
		byte[] srcData = email.getBytes("UTF-8");
		byte[] tarData = compress(srcData, 0, srcData.length, 5);
		
		System.out.println(srcData.length + "  "  + tarData.length);
		
	}
	
	public static long SortableStr2long(String sval, int offset, int len) {
	    long val = (long)(sval.charAt(offset++)) << 60;
	    val |= ((long)sval.charAt(offset++)) << 45;
	    val |= ((long)sval.charAt(offset++)) << 30;
	    val |= sval.charAt(offset++) << 15;
	    val |= sval.charAt(offset);
	    val -= Long.MIN_VALUE;
	    return val;
	  }
	
	public static byte[] compress(byte[] value, int offset, int length, int compressionLevel) {
	    ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
	    Deflater compressor = new Deflater();

	    try {
	      compressor.setLevel(compressionLevel);
	      compressor.setInput(value, offset, length);
	      compressor.finish();

	      // Compress the data
	      final byte[] buf = new byte[1024];
	      while (!compressor.finished()) {
	        int count = compressor.deflate(buf);
	        bos.write(buf, 0, count);
	      }
	    } finally {
	      compressor.end();
	    }

	    return bos.toByteArray();
	  }
}
