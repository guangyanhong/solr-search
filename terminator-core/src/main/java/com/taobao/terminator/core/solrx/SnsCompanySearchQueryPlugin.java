/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrQueryParser;



/**
 * @author <a href="mailto:xiangfei@taobao.com"> xiangfei</a>
 * @version 2010-3-1:下午01:35:59
 * 
 */
public class SnsCompanySearchQueryPlugin extends QParserPlugin {

	private static Log log = LogFactory
			.getLog(SnsCompanySearchQueryPlugin.class);
	SolrQueryParser lparser = null;

	@Override
	public QParser createParser(String qstr, SolrParams localParams,
			SolrParams params, SolrQueryRequest req) {
		return new QParser(qstr, localParams, params, req) {
			@SuppressWarnings("deprecation")
			public Query parse() throws ParseException {
				BooleanQuery booleanQuery;

				// 1. 生成luceneParser
				String defaultField = getParam(CommonParams.DF);
				if (defaultField == null) {
					defaultField = getReq().getSchema()
							.getDefaultSearchFieldName();
				}
				lparser = new SolrQueryParser(this, defaultField);

				// these could either be checked & set here, or in the
				// SolrQueryParser constructor
				String opParam = getParam(QueryParsing.OP);
				if (opParam != null) {
					lparser
							.setDefaultOperator("AND".equals(opParam) ? QueryParser.Operator.AND
									: QueryParser.Operator.OR);
				} else {
					// try to get default operator from schema
					QueryParser.Operator operator = getReq().getSchema()
							.getSolrQueryParser(null).getDefaultOperator();
					lparser
							.setDefaultOperator(null == operator ? QueryParser.Operator.OR
									: operator);
				}

				// 2. 提交给lucene进行分析
				// 允许在进行payloadQuery查询时，不需要额外的查询参数，这时就不需要提交给lucene进行分析了
				Query luceneQuery = null;
				if (qstr != null && qstr.length() > 0) {
					luceneQuery = lparser.parse(qstr);
				}

				if (luceneQuery == null) {
					// 2.1 只需要封装payloadquery
					booleanQuery = new BooleanQuery();

				} else if (luceneQuery instanceof BooleanQuery) {
					// 2.2 除payloadquery外的条件，经lucene分析是一个boolean查询
					booleanQuery = (BooleanQuery) luceneQuery;
				} else {
					// 2.3
					// 除payloadquery外的条件，经lucene分析出来的不是一个boolean查询，这时应该把它放入boolean
					// query
					booleanQuery = new BooleanQuery();
					booleanQuery.add(luceneQuery, Occur.MUST);
				}

				int slopNum = 10;
				String field = localParams.get(CommonParams.FL);
				String companyName = localParams.get(field);
				String slop = localParams.get("slop");
				if (slop != null) {
					try {
						slopNum = Integer.valueOf(slop);
					} catch (Exception e) {
						log.warn("slop值不是number类型，使用默认值10");

					}
				}
				// 没有指明要payloadtermquery查询,则可以直接返回
				if (companyName == null) {
					return booleanQuery;
				}

				StandardAnalyzer standardAnalyzer = new StandardAnalyzer(
						Version.LUCENE_CURRENT);

				TokenStream ts = standardAnalyzer.tokenStream("sns",
						new StringReader(companyName));

				TermAttribute termAtt = (TermAttribute) ts
						.getAttribute(TermAttribute.class);

				List<SpanTermQuery> spanTermQ = new ArrayList<SpanTermQuery>();
				try {
					while (ts.incrementToken()) {
						String term = termAtt.term();
						SpanTermQuery spanTermQuery = new SpanTermQuery(
								new Term(field, term));
						spanTermQ.add(spanTermQuery);
					}
				} catch (IOException e) {
					log.warn("解释输入内容时出错:" + companyName);
					throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
							"cant parse the content: " + companyName);
				}

				SpanQuery[] spanQuery = null;
				if(spanTermQ != null && spanTermQ.size() > 0){
					spanQuery = new SpanQuery[spanTermQ.size()];					
				}
				
				for(int i = 0 ; i < spanTermQ.size() ; i++){
					spanQuery[i] = spanTermQ.get(i);
				}
				
				SpanNearQuery snq = new SpanNearQuery(spanQuery, slopNum, true);

				Query query = null;
				// 合并两部分查询
				if (booleanQuery.getClauses() == null
						|| booleanQuery.getClauses().length == 0) {
					query = snq;

				} else {
					booleanQuery.add(snq, Occur.MUST);
					query = booleanQuery;
				}

				return query;
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) {
		
	}

}
