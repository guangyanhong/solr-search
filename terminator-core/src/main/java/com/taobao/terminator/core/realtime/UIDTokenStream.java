package com.taobao.terminator.core.realtime;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Payload;

/**
 * 为了一次性取出所有的UID->DocID的映射用Payload的方式存储UID
 * 
 * @author yusen
 *
 */
public class UIDTokenStream extends TokenStream {
	
	private boolean returnToken = false;

	private PayloadAttribute payloadAttr;
	private TermAttribute termAttr;

	public UIDTokenStream(long uid) {
		byte[] buffer = new byte[8];
		
		buffer[0] = (byte) (uid);
		buffer[1] = (byte) (uid >> 8);
		buffer[2] = (byte) (uid >> 16);
		buffer[3] = (byte) (uid >> 24);
		buffer[4] = (byte) (uid >> 32);
		buffer[5] = (byte) (uid >> 40);
		buffer[6] = (byte) (uid >> 48);
		buffer[7] = (byte) (uid >> 56);
		
		payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
		payloadAttr.setPayload(new Payload(buffer));
		
		termAttr = (TermAttribute) addAttribute(TermAttribute.class);
		termAttr.setTermBuffer(TerminatorIndexReader.TERM_VALUE);
		
		returnToken = true;
	}

	@Override
	public boolean incrementToken() throws IOException {
		if (returnToken) {
			returnToken = false;
			return true;
		} else {
			return false;
		}
	}
}