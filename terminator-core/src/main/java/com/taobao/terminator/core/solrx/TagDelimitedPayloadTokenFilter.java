
/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Payload;

/**
 * 这个类对原来<code>DelimitedPayloadTokenFilter</code>类改进的地方有：
 * 1)term内容中可以包含分割符的内容
 * 2)对termAtt的扫描由原来的从左到右扫描改为从右到左扫描，这样可以在一定程序上提高效率，
 * 因为一般来说term内容的长度要比payload的长,如果  诺基亚|5.0
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version 2010-2-26:下午03:53:48
 * 
 */
public class TagDelimitedPayloadTokenFilter extends TokenFilter {

	public static final char DEFAULT_DELIMITER = '|';
	protected char delimiter = DEFAULT_DELIMITER;
	protected TermAttribute termAtt;
	protected PayloadAttribute payAtt;
	protected PayloadEncoder encoder;
	protected Payload pl;

	/**
	 * Construct a token stream filtering the given input.
	 */
	protected TagDelimitedPayloadTokenFilter(TokenStream input) {
		this(input, DEFAULT_DELIMITER, new IdentityEncoder());
	}

	public TagDelimitedPayloadTokenFilter(TokenStream input, char delimiter,
			PayloadEncoder encoder) {
		super(input);
		termAtt = (TermAttribute) addAttribute(TermAttribute.class);
		payAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
		this.delimiter = delimiter;
		this.encoder = encoder;
	}

	@Override
	public boolean incrementToken() throws IOException {
		boolean result = false;
		if (input.incrementToken()) {
			final char[] buffer = termAtt.termBuffer();
			final int length = termAtt.termLength();
			// look for the delimiter
			boolean seen = false;
			for (int i = length -1  ; i >= 0; i--) {
				if (buffer[i] == delimiter) {

					termAtt.setTermBuffer(buffer, 0, i);
					try {
						pl = encoder.encode(buffer, i + 1, (length - (i + 1)));
						payAtt.setPayload(pl);
						seen = true;
						break;// at this point, we know the whole piece, so we
						// can exit. If we don't see the delimiter, then
						// the termAtt is the same

					} catch (Exception e) {
						//break 
						termAtt.setTermBuffer(buffer, 0, length);
                          break;
					}
				}
			}
			if (seen == false) {
				// no delimiter
				payAtt.setPayload(null);
			}
			result = true;
		}
		return result;
	}
}
