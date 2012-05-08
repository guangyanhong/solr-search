/**
 *  Rights reserved by www.taobao.com
 */
package com.taobao.terminator.core.solrx;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
 * 这个Class以前在Tstart的Dump的jar包里，现在移到Core的solrx包中
 * 2010-12-11 16:31 移动的
 */
public class TstarQParserPlugin extends QParserPlugin {

	PayloadFunction payloadFunction;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.solr.util.plugin.NamedListInitializedPlugin#init(org.apache
	 * .solr.common.util.NamedList)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void init(NamedList args) {
		String func = (String) args.get("func");
		payloadFunction = createPayloadFunction(func);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.solr.search.QParserPlugin#createParser(java.lang.String,
	 * org.apache.solr.common.params.SolrParams,
	 * org.apache.solr.common.params.SolrParams,
	 * org.apache.solr.request.SolrQueryRequest)
	 */
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

		@SuppressWarnings("deprecation")
		@Override
		public Query parse() throws ParseException {
			BooleanQuery booleanQuery;

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

			Query luceneQuery = null;
			if (qstr != null && qstr.length() > 0&&!qstr.trim().equals("")) {
				luceneQuery = lparser.parse(qstr);
			}

			if (luceneQuery == null) {
				booleanQuery = new BooleanQuery();
			} else if (luceneQuery instanceof BooleanQuery) {
				booleanQuery = (BooleanQuery) luceneQuery;
			} else {
				booleanQuery = new BooleanQuery();
				booleanQuery.add(luceneQuery, Occur.MUST);
			}

			
			String des1Param = localParams.get("des1");
			String des2Param = localParams.get("des2");
			if (des1Param == null && des2Param==null) {
				return booleanQuery;
			}

			BooleanQuery payloadBooleanQuery = new BooleanQuery();
			if(des1Param!=null&&!des1Param.trim().equals("")){
				String[] tagArray = des1Param.split(" ");
				for (int i = 0; i < tagArray.length && !tagArray[i].equals(" "); i++) {
					Object[] termAtt = parseTerm(tagArray[i]);
					PayloadTermQuery payloadTermQuery = new PayloadTermQuery(new Term("des1", (String) termAtt[0]),createPayloadFunction("max"));
					payloadTermQuery.setBoost((Float) termAtt[1]);
					payloadBooleanQuery.add(payloadTermQuery, Occur.SHOULD);
				}
			}

			if(des2Param!=null&&!des2Param.trim().equals("")){
				Query q = lparser.parse("des2:"+des2Param.replaceAll(" ", ""));
				if(q instanceof PhraseQuery){
					PhraseQuery fq = (PhraseQuery)q;
					Term[] ts = fq.getTerms();
					for(Term t : ts){
						if(t!=null){
							payloadBooleanQuery.add(new TermQuery(t),Occur.SHOULD);
						}
					}
				}else{
					payloadBooleanQuery.add(lparser.parse("des2:"+des2Param.replaceAll(" ", "")),Occur.SHOULD);
				}
			}
			
			if (booleanQuery.getClauses() == null || booleanQuery.getClauses().length == 0) {
				booleanQuery = payloadBooleanQuery;
			} else {
				booleanQuery.add(payloadBooleanQuery, Occur.MUST);
			}

			return booleanQuery;
		}
	}

	private PayloadFunction createPayloadFunction(String func) {

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
