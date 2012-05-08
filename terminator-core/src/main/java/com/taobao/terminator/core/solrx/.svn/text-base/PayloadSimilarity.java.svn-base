/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.DefaultSimilarity;

/**
 * 
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version 2010-2-26:ÉÏÎç09:56:48
 * 
 */
public class PayloadSimilarity extends DefaultSimilarity {
	private static final long serialVersionUID = 1L;

	@Override
	public float scorePayload(int docId, String fieldName, int start, int end, byte[] payload, int offset, int length) {
		return PayloadHelper.decodeFloat(payload, offset);// we can ignore
	}

	@Override
	public float coord(int overlap, int maxOverlap) {
		return 1.0f;
	}

}
