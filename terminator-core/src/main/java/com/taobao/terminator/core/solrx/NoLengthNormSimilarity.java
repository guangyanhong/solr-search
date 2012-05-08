/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import org.apache.lucene.search.DefaultSimilarity;

/**
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version 2010-3-25:обнГ04:17:12
 * 
 */
public class NoLengthNormSimilarity extends DefaultSimilarity {

	private static final long serialVersionUID = -4299083054223717519L;

	/** Implemented as <code>1/sqrt(numTerms)</code>. */
	public float lengthNorm(String fieldName, int numTerms) {
		return 1.0f;
	}

}
