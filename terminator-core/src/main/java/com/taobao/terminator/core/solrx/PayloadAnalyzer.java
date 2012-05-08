/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;

/**
 * 这个类的主要作用用来分析那些带权重的内容。
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version      2010-2-26:下午04:56:10
 *
 */
public class PayloadAnalyzer extends Analyzer {

	private PayloadEncoder encoder = new  FloatEncoder();

	public PayloadAnalyzer(PayloadEncoder encoder) {
		this.encoder = encoder;

	}
	
	public PayloadAnalyzer(){}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new WhitespaceTokenizer(reader);
		result = new TagDelimitedPayloadTokenFilter(result, '^', encoder);
		return result;
	}
}
