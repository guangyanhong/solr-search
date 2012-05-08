package com.taobao.terminator.core.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.AttributeSource;

public class PayloadAnalyzerTest {
	private static final String[] examples = {
			"The quick brown fox jumped over the lazy dogs",
			"XY&Z Corporation - xyz@example.com" };
	private static final Analyzer[] analyzers = new Analyzer[] {
			new WhitespaceAnalyzer(), new SimpleAnalyzer(), new StopAnalyzer(),
			new StandardAnalyzer() };

	public static void main(String[] args) throws IOException {
		// Use the embedded example strings, unless
		// command line arguments are specified, then use those.
		String[] strings = examples;
		if (args.length > 0) {
			strings = args;
		}
//		for (int i = 0; i < strings.length; i++) {?
			analyze("The quick brown fox jumped over the lazy dogs");
//		}
	}

	private static void analyze(String text) throws IOException {
		System.out.println("Analyzing \"" + text + "\"");
		for (int i = 0; i < analyzers.length; i++) {
			Analyzer analyzer = analyzers[i];
			String name = analyzer.getClass().getName();
			name = name.substring(name.lastIndexOf(".") + 1);
			System.out.println(" " + name + ":");
			System.out.print(" ");
			AnalyzerUtils.displayTokens(analyzer, text);
			System.out.println("\n");
		}
	}

	static public class AnalyzerUtils {
		public static AttributeSource[] tokensFromAnalysis(Analyzer analyzer,
				String text) throws IOException {
			TokenStream stream = analyzer.tokenStream("contents", new StringReader(text)); 
			ArrayList tokenList = new ArrayList();
			while (true) {
				if (!stream.incrementToken())
					break;
				tokenList.add(stream.captureState());
			}
			return (AttributeSource[]) tokenList.toArray(new AttributeSource[tokenList.size()]);
		}

		public static void displayTokens(Analyzer analyzer, String text)
				throws IOException {
			AttributeSource[] tokens = tokensFromAnalysis(analyzer, text);
			for (int i = 0; i < tokens.length; i++) {
				AttributeSource token = tokens[i];
				TermAttribute term = (TermAttribute) token
						.addAttribute(TermAttribute.class);
				System.out.print("[" + term.term() + "] "); // 2
			}
		}
	}
}