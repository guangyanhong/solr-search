/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.payloads.AveragePayloadFunction;
import org.apache.lucene.search.payloads.MaxPayloadFunction;
import org.apache.lucene.search.payloads.MinPayloadFunction;
import org.apache.lucene.search.payloads.PayloadFunction;
import org.apache.lucene.search.payloads.PayloadTermQuery;
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
 * @version 2010-2-26:上午10:01:28
 * 
 */
public class PayloadTermQueryPlugin extends QParserPlugin {

	PayloadFunction payloadFunction;

	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) {
		String func = (String) args.get("func");
		payloadFunction = createPayloadFunction(func);

	}

	@Override
	public QParser createParser(String qstr, SolrParams localParams,
			SolrParams params, SolrQueryRequest req) {
		return new PayloadQParser(qstr, localParams, params, req);
	}

	class PayloadQParser extends QParser {
		SolrQueryParser lparser;

		public PayloadQParser(String qstr, SolrParams localParams,
				SolrParams params, SolrQueryRequest req) {
			super(qstr, localParams, params, req);
		}

		@Override
		public Query parse() throws ParseException {
			BooleanQuery booleanQuery;

			// 1. 生成luceneParser
			String defaultField = getParam(CommonParams.DF);
			if (defaultField == null) {
				defaultField = getReq().getSchema().getDefaultSearchFieldName();
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

			// {!payload tags='ibm^4 apple^5'}+productType:((1) (2))
			String tags = localParams.get("tags");
			// 没有指明要payloadtermquery查询,则可以直接返回
			if (tags == null) {
				return booleanQuery;
			}

			// 3. new payloadtermquery

			String[] tagArray = tags.split(" ");
			BooleanQuery payloadBooleanQuery = new BooleanQuery();
			for (int i = 0; i < tagArray.length && !tagArray[i].equals(" "); i++) {

				// 分析一下term中是否包含payload，如果有，要先分析出来
				Object[] termAtt = parseTerm(tagArray[i]);
				PayloadTermQuery payloadTermQuery = new PayloadTermQuery(
						new Term("tags", (String) termAtt[0]),
						createPayloadFunction("max"));
				payloadTermQuery.setBoost((Float) termAtt[1]);
				// 将这些payloadtermquery组装成一个boolean查询
				payloadBooleanQuery.add(payloadTermQuery, Occur.SHOULD);
			}
			// 合并两部分查询
			if (booleanQuery.getClauses() == null
					|| booleanQuery.getClauses().length == 0) {
				booleanQuery = payloadBooleanQuery;
			} else {
				booleanQuery.add(payloadBooleanQuery, Occur.MUST);
			}

			return booleanQuery;
		}
	}

	private PayloadFunction createPayloadFunction(String func) {

		// default payloadFunction is MaxPayloadFunction

		PayloadFunction payloadFunction = null;

		if (payloadFunction == null) {
			payloadFunction = new MaxPayloadFunction();
		} else if ("min".equals(func)) {
			payloadFunction = new MinPayloadFunction();
		} else if ("avg".equals(func)) {
			payloadFunction = new AveragePayloadFunction();
		} else if ("max".equals(func)) {
			payloadFunction = new MaxPayloadFunction();
		} else {

			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
					"unknown PayloadFunction: " + func);
		}

		return payloadFunction;
	}

	private Object[] parseTerm(String term) {
		int length = term.length();
		boolean flag = true;
		Object[] arr = new Object[2];
		int index = term.lastIndexOf('^');
		if (index < 0 || index >= length) {
			arr[0] = term;
			arr[1] = 1.0f;
		} else {
			String boost = term.substring(index + 1);
			float fboost = 1.0f;
			try {
				fboost = Float.parseFloat(boost);
			} catch (Exception e) {
				// FIXME
				System.out.println(e.getMessage());
				flag = false;
				arr[0] = term;
			}
			if (flag) {
				arr[0] = term.substring(0, index);
			}
			arr[1] = fboost;
		}

		return arr;

	}

}
