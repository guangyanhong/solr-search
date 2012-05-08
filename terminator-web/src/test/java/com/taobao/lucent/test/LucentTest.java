package com.taobao.lucent.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.payloads.PayloadFunction;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LucentTest {
	
	public static Directory dir = null;
	
	static {
		try {
			dir = FSDirectory.open(new File("C:\\index"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void write() throws Exception {
		
		IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), MaxFieldLength.UNLIMITED);
		for (int i = 0; i < 100; i++) {
			Document doc = new Document();
			Field f = new Field("id", "" + i, Store.YES, Index.NOT_ANALYZED_NO_NORMS);
			Field pf = new Field("count", "" + i, Store.YES, Index.NOT_ANALYZED_NO_NORMS);
			Field tf = new Field("time", new IntRangeTokenStream(i, "_TIME_"));
			doc.add(f);
			doc.add(pf);
			doc.add(tf);
			writer.addDocument(doc);
		}
		Map<String, String> commitUserData = new HashMap<String, String>();
		commitUserData.put("author", "yusen");
		writer.commit(commitUserData);
	}

	public static void search() throws Exception {
		IndexReader indexReader = IndexReader.open(dir, false);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		searcher.setSimilarity(new MySimilarity());
		TermQuery tq1 = new TermQuery(new Term("id", "1"));
		TermQuery tq2 = new TermQuery(new Term("id", "2"));
		
		BooleanQuery q = new BooleanQuery();
		PayloadTermQuery pq = new PayloadTermQuery(new Term("time", "_TIME_"), new MyPayloadFunc());
		q.add(pq, Occur.SHOULD);
		
		Hits hits = searcher.search(q);
		System.out.println(hits.length());
	}

	public static void main(String[] args) throws Exception {
//		write();
		search();
	}

	public static class MyPayloadFunc extends PayloadFunction {

		public float currentScore(int docId, String field, int start, int end, int numPayloadsSeen, float currentScore, float currentPayloadScore) {
			return currentPayloadScore + currentScore;
		}

		public float docScore(int docId, String field, int numPayloadsSeen, float payloadScore) {
			if (docId == 1) {
				return 2;
			} else {
				return 1;
			}
		}

		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.getClass().hashCode();
			return result;
		}

		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			return true;
		}
	}
	
	public static class MySimilarity extends Similarity {
		
		public float scorePayload(int docId, String fieldName, int start, int end, byte [] payload, int offset, int length) {
			return 1;
		}

		public float scorePayload(String fieldName, byte[] payload, int offset, int length) {
			return 1;
		}

		@Override
		public float tf(float freq) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float sloppyFreq(int distance) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float queryNorm(float sumOfSquaredWeights) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float lengthNorm(String fieldName, int numTokens) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float idf(int docFreq, int numDocs) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public float coord(int overlap, int maxOverlap) {
			// TODO Auto-generated method stub
			return 0;
		}
	
	}

	public static class IntRangeTokenStream extends TokenStream {

		private boolean returnToken = false;

		private PayloadAttribute payloadAttr;
		private TermAttribute termAttr;

		public IntRangeTokenStream(int num, String value) {
			byte[] buffer = new byte[4];

			buffer[0] = (byte) (num);
			buffer[1] = (byte) (num >> 8);
			buffer[2] = (byte) (num >> 16);
			buffer[3] = (byte) (num >> 24);

			payloadAttr = (PayloadAttribute) addAttribute(PayloadAttribute.class);
			payloadAttr.setPayload(new Payload(buffer));

			termAttr = (TermAttribute) addAttribute(TermAttribute.class);
			termAttr.setTermBuffer(value);

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
}
