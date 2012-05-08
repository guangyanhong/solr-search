package com.taobao.terminator.core.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class IndexWriteTest {
	public static void maisn(String[] args) throws IOException {
		File file = new File("D:\\lucene-index-test");
		Directory dir = FSDirectory.open(file);
		Analyzer analyzer = new ChineseAnalyzer();
		/*
		 * IndexWriter writer = new
		 * IndexWriter(dir,analyzer,true,MaxFieldLength.UNLIMITED); Document
		 * doc1 = new Document(); doc1.add(new
		 * Field("content","我是中国人",Store.YES,Index.ANALYZED,TermVector.YES));
		 * writer.addDocument(doc1);
		 * 
		 * Document doc2 = new Document(); doc2.add(new
		 * Field("content","我是中人国家大事",Store.YES,Index.ANALYZED,TermVector.YES));
		 * writer.addDocument(doc2);
		 * 
		 * Document doc3 = new Document(); doc3.add(new
		 * Field("content","我是中哈哈人国家大事"
		 * ,Store.YES,Index.ANALYZED,TermVector.YES)); writer.addDocument(doc3);
		 * 
		 * writer.commit(); writer.optimize(); writer.close();
		 */

		IndexSearcher searcher = new IndexSearcher(dir, true);
		// Weight weight, Filter filter, final int nDocs

		// public void search(Query query, Collector results)
		Query query = new TermQuery(new Term("content", "国"));

		SpanQuery termQ1 = new SpanTermQuery(new Term("content", "中"));
		SpanQuery termQ2 = new SpanTermQuery(new Term("content", "国"));

		// public SpanNearQuery(SpanQuery[] clauses, int slop, boolean inOrder)
		// {
		Query spanQuery = new SpanNearQuery(new SpanQuery[] { termQ2, termQ2 },
				0, true);
		Hits hits = searcher.search(spanQuery);
		Iterator i = hits.iterator();
		while (i.hasNext()) {
			Hit h = (Hit) i.next();
			System.out.println(h.getDocument());
			System.out.println(h.getScore());
		}

		/*
		 * IndexReader reader = IndexReader.open(dir,true);
		 * System.out.println(reader.maxDoc()); TermDocs ts =
		 * reader.termDocs(new Term("content","国")); while(ts.next()){
		 * System.out.println(ts.doc()); }
		 * 
		 * TermFreqVector tf = reader.getTermFreqVector(21, "content");
		 * System.out.println(tf); System.out.println(tf.indexOf("国"));
		 */
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception, IOException {
		RAMDirectory directory;
		IndexSearcher searcher;
		IndexReader reader;
		SpanTermQuery quick;
		SpanTermQuery brown;
		SpanTermQuery red;
		SpanTermQuery fox;
		SpanTermQuery lazy;
		SpanTermQuery sleepy;
		SpanTermQuery dog;
		SpanTermQuery cat;
		Analyzer analyzer;
		directory = new RAMDirectory();
		analyzer = new ChineseAnalyzer();
		IndexWriter writer = new IndexWriter(directory, analyzer,IndexWriter.MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("f", "我是中国人",Field.Store.YES, Field.Index.ANALYZED));
		writer.addDocument(doc);
		
		doc = new Document();
		doc.add(new Field("f", "我在中间的国家",Field.Store.YES, Field.Index.ANALYZED));
		writer.addDocument(doc);
		doc = new Document();
		doc.add(new Field("f", "我在的国家是在亚洲的中部",Field.Store.YES, Field.Index.ANALYZED));
		writer.addDocument(doc);
		writer.close();
		
		searcher = new IndexSearcher(directory);
		reader = IndexReader.open(directory);
		quick = new SpanTermQuery(new Term("f", "quick"));
		brown = new SpanTermQuery(new Term("f", "brown"));
		red = new SpanTermQuery(new Term("f", "red"));
		fox = new SpanTermQuery(new Term("f", "fox"));
		lazy = new SpanTermQuery(new Term("f", "lazy"));
		sleepy = new SpanTermQuery(new Term("f", "sleepy"));
		dog = new SpanTermQuery(new Term("f", "dog"));
		cat = new SpanTermQuery(new Term("f", "cat"));
		
		PhraseQuery pq = new PhraseQuery();
		pq.add(new Term("f","中"));
		pq.add(new Term("f","国"));
		pq.setSlop(2);
		
		QueryParser paser = new QueryParser("f",analyzer);
		paser.setDefaultOperator(Operator.OR);
		SpanFirstQuery sfq = new SpanFirstQuery(brown, 3);
		
		SpanQuery t1 = new SpanTermQuery(new Term("f","中"));
		SpanQuery t2 = new SpanTermQuery(new Term("f","国"));
		SpanNearQuery snq = new SpanNearQuery(new SpanQuery[]{t1,t2}, 6, false);
		Hits hits = searcher.search(snq);
		Iterator i = hits.iterator();
		while (i.hasNext()) {
			Hit h = (Hit) i.next();
			System.out.println(h.getDocument());
			System.out.println(h.getScore());
		}

	}
}
