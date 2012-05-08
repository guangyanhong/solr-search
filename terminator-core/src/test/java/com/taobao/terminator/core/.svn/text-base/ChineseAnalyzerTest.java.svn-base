package com.taobao.terminator.core;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;

public class ChineseAnalyzerTest {
	public static void main(String[] args) throws IOException {
		ChineseAnalyzer an = new ChineseAnalyzer();
		TokenStream ts = an.tokenStream("name", new StringReader("123"));
		Token t = null;
		while((t = ts.next())!= null){
			System.out.println(t.term());
		}
		
	}
}	
