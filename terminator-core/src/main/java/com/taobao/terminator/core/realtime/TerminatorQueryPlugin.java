package com.taobao.terminator.core.realtime;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrQueryParser;

import com.taobao.terminator.core.realtime.DefaultSearchService.SortFieldThreadLocalContext;

/**
 * 调用方传过来的QueryString，默认先用Lucene的Parser进行语法解析，为了配合Payload方式的排序优化，在Lucene解析的最终Query对象基础上强制加上
 * PayloadTermQuery _SFS_:_SFS_VALUE_，_SFS_这个Field在每个Doc中均存在且值都为_SFS_VALUE_,加上这个条件的目的是为了保证Lucene在计算分值的
 * 时候能调用到Similarity的scorePayload()方法，已达到干涉打分排序的目的。
 * 
 * @author yusen
 */
public class TerminatorQueryPlugin extends QParserPlugin {

	@Override
	public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
		 return new TerminatorQParser(qstr, localParams, params, req);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) { }

	class TerminatorQParser extends QParser {
		String sortStr;
		SolrQueryParser lparser;

		public TerminatorQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
			super(qstr, localParams, params, req);
		}

		@SuppressWarnings("deprecation")
		public Query parse() throws ParseException {
			String qstr = getString();

			String defaultField = getParam(CommonParams.DF);
			if (defaultField == null) {
				defaultField = getReq().getSchema().getDefaultSearchFieldName();
			}
			lparser = new SolrQueryParser(this, defaultField);

			String opParam = getParam(QueryParsing.OP);
			if (opParam != null) {
				lparser.setDefaultOperator("AND".equals(opParam) ? QueryParser.Operator.AND : QueryParser.Operator.OR);
			} else {
				QueryParser.Operator operator = getReq().getSchema().getSolrQueryParser(null).getDefaultOperator();
				lparser.setDefaultOperator(null == operator ? QueryParser.Operator.OR : operator);
			}

			Query luceneQuery = lparser.parse(qstr);
			
			PayloadTermQuery pq = new PayloadTermQuery(new Term("_SFS_", "_SFS_VALUE_"), new AveragePayloadFunction());
			
			String sf = SortFieldThreadLocalContext.getInstance().get();
			if(sf == null) {
				return luceneQuery;
			} else {
				BooleanQuery booleanQuery;
				if (luceneQuery == null) {
					booleanQuery = new BooleanQuery();
					booleanQuery.add(pq,Occur.MUST);
					return booleanQuery;
				} else if (luceneQuery instanceof BooleanQuery) {
					BooleanQuery bq = new BooleanQuery();
					bq.add(luceneQuery,Occur.MUST);
					bq.add(pq,Occur.MUST);
					return bq;
				} else {
					booleanQuery = new BooleanQuery();
					booleanQuery.add(luceneQuery, Occur.MUST);
					booleanQuery.add(pq,Occur.MUST);
					return booleanQuery;
				}
			}
		}

		public String[] getDefaultHighlightFields() {
			return new String[] { lparser.getField() };
		}
	}
}
