package com.taobao.terminator.web.perftest.analyzer;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class DelimiterAnanlyzer extends Analyzer {
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new DelimiterTokenizer(reader);
	}

	public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
		Tokenizer tokenizer = (Tokenizer) getPreviousTokenStream();
		if (tokenizer == null) {
			tokenizer = new DelimiterTokenizer(reader);
			setPreviousTokenStream(tokenizer);
		} else
			tokenizer.reset(reader);
		return tokenizer;
	}
}
