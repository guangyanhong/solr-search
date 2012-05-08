package com.taobao.terminator.core.realtime;

import org.apache.lucene.search.Similarity;
import org.apache.solr.schema.SimilarityFactory;

public class PayloadSimilarityFactory extends SimilarityFactory{

	@Override
	public Similarity getSimilarity() {
		return new PayloadSimilarity(params);
	}
}
