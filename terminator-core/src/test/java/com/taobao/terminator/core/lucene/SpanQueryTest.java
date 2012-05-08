package com.taobao.terminator.core.lucene;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

public class SpanQueryTest{
	private RAMDirectory directory;
	private IndexSearcher searcher;
	private IndexReader reader;
	private SpanTermQuery quick;
	private SpanTermQuery brown;
	private SpanTermQuery red;
	private SpanTermQuery fox;
	private SpanTermQuery lazy;
	private SpanTermQuery sleepy;
	private SpanTermQuery dog;
	private SpanTermQuery cat;
	private Analyzer analyzer;

	@Before
	public void setUp() throws Exception {
		directory = new RAMDirectory();
		analyzer = new WhitespaceAnalyzer();
		IndexWriter writer = new IndexWriter(directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("f", "the quick brown fox jumps over the lazy dog", Field.Store.YES, Field.Index.ANALYZED));
		writer.addDocument(doc);
		
		doc = new Document();
		doc.add(new Field("f", "the quick red fox jumps over the sleepy cat", Field.Store.YES, Field.Index.ANALYZED));
		writer.addDocument(doc);
		writer.close();
		
		searcher = new IndexSearcher(directory);
		reader = IndexReader.open(directory);
		quick  = new SpanTermQuery(new Term("f", "quick"));
		brown  = new SpanTermQuery(new Term("f", "brown"));
		red    = new SpanTermQuery(new Term("f", "red"));
		fox    = new SpanTermQuery(new Term("f", "fox"));
		lazy   = new SpanTermQuery(new Term("f", "lazy"));
		sleepy = new SpanTermQuery(new Term("f", "sleepy"));
		dog    = new SpanTermQuery(new Term("f", "dog"));
		cat    = new SpanTermQuery(new Term("f", "cat"));
	}

	@Test
	public void assertOnlyBrownFox() throws Exception {
		TopDocs hits = searcher.search(brown, 10);
		assertEquals(1, hits.totalHits);
		assertEquals("wrong doc", 0, hits.scoreDocs[0].doc);
	}

	public void assertBothFoxes(Query query) throws Exception {
		TopDocs hits = searcher.search(query, 10);
		assertEquals(2, hits.totalHits);
	}

	public void assertNoMatches(Query query) throws Exception {
		TopDocs hits = searcher.search(query, 10);
		assertEquals(0, hits.totalHits);
	}
}