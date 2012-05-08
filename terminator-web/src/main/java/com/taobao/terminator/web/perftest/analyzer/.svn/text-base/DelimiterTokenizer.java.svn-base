package com.taobao.terminator.web.perftest.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.util.AttributeSource;

public class DelimiterTokenizer extends CharTokenizer {
	public DelimiterTokenizer(Reader in) {
		super(in);
	}

	public DelimiterTokenizer(AttributeSource source, Reader in) {
		super(source, in);
	}

	public DelimiterTokenizer(AttributeFactory factory, Reader in) {
		super(factory, in);
	}

	protected boolean isTokenChar(char c) {
		return '|' != c;
	}
}
