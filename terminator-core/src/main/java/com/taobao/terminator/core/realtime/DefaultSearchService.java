package com.taobao.terminator.core.realtime;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

import com.taobao.terminator.common.CoreProperties;
import com.taobao.terminator.common.TerminatorCommonUtils;
import com.taobao.terminator.common.TerminatorConstant;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.SearchService;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.core.fieldx.RangeField;

public class DefaultSearchService implements SearchService,SelfPublisher{
	protected static Log log = LogFactory.getLog(SearchService.class);
	protected SolrServer solrServer;
	protected SolrCore solrCore;
	
	public DefaultSearchService(SolrCore solrCore){
		this.solrCore = solrCore;
		this.solrServer = new EmbeddedSolrServer(solrCore.getCoreDescriptor().getCoreContainer(),solrCore.getName());
	}
	
	@Override
	public void publishHsfService(String coreName) {
		CoreProperties coreProperties = null;
		try {
			coreProperties = new CoreProperties(solrCore.getResourceLoader().openConfig("core.properties"));
		} catch (Exception e) {
			RuntimeException re = new RuntimeException("Read [" + coreName +"]'s core.properties Error!",e);
			log.error(re,re);
			throw re;
		}
		
		if(coreProperties.isReader()){
			
			try {
                //TODO
//				String version = null;
//				HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//				providerBean.setTarget(this);
//				providerBean.setServiceInterface(SearchService.class.getName());
//				providerBean.setServiceVersion(version = coreName + "-reader");
//				providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//				providerBean.setClientTimeout(30000);
//
//				log.warn("Publish SerachService version is {" + version + "}");
//
//				providerBean.afterPropertiesSet();
			} catch (Exception e) {
				log.error("Publish HSF Error",e);
				throw new RuntimeException("Publish HSF Error",e);
			}
		}
		
		if(coreProperties.isMerger()) {
			try {
                //TODO
//				String version = null;
//				HSFSpringProviderBean providerBean = new HSFSpringProviderBean();
//				providerBean.setTarget(this);
//				providerBean.setServiceInterface(SearchService.class.getName());
//				providerBean.setServiceVersion( version = TerminatorCommonUtils.splitCoreName(coreName)[0] + "-merger");
//				providerBean.setSerializeType(TerminatorConstant.DEFAULT_SERIALIZE_TYPE);
//				providerBean.setClientTimeout(3000);
//
//				log.warn("Publish SerachService version is {" + version + "}");
//
//				providerBean.afterPropertiesSet();
			} catch (Exception e) {
				log.error("Publish HSF Error",e);
				throw new RuntimeException("Publish HSF Error",e);
			}
		}
	}
	
	@Deprecated
	public QueryResponse query2(TerminatorQueryRequest query) throws TerminatorServiceException {

		long startTime = System.currentTimeMillis();
		QueryResponse resp = null;
		try {
			resp =  solrServer.query(query);
		} catch (Exception e) {
			throw new TerminatorServiceException(e);
		}
		
		long totalTime = System.currentTimeMillis() - startTime;
		if(totalTime >= 3 * 1000){
			log.warn("Queryʱ�������ִ�е�Request ==> " + query.toString() + " ִ��ʱ�� ==> " + totalTime);
		}else if(log.isDebugEnabled()){
			log.debug("ִ�е�Request ==> " + query.toString() + " ִ��ʱ�� ==> " + totalTime);
		}
		return resp;
	}
	
	
	public static void main(String[] args) {
		String query2 = "s_id:175754667  AND NOT status:\\-4";
		System.out.println(query2.replaceAll("AND\\sNOT\\sstatus:\\\\-4", " ").replace("s_id:175754667", " "));
	}
	
	/**
	 * fuweiwei���ӣ�ecrm�����������ѯ
	 * @param rq �ͻ������õ�rangeQueryֵ
	 * @param sb �洢ֵ
	 */
	private void transforRangeToQuery(String rq, StringBuilder sb) {
		try {
			Range2 r = Range2.parse(rq);
			if (r.includeStart == r.includeEnd) {
				sb.append(" AND ").append(rq);
			} else {
				sb.append(" AND ").append(r.fn + ":[" + r.start + " TO " + r.end + "]");
				if (r.includeStart) {
					if(!"*".equals(r.end)) {
						sb.append(" AND ").append(" NOT ").append(r.fn + ":" + r.end);
					}
				} else {
					sb.append(" AND ").append(" NOT ").append(r.fn + ":" + r.start);
				}
			}

		} catch (TerminatorServiceException e) {
			e.printStackTrace();
		}
	}
	@Override
	public QueryResponse query(TerminatorQueryRequest query) throws TerminatorServiceException {
		boolean hasRange = false;
		boolean hasSortField = false;
		try {
			String[] rangeQuery = query.getRangeQuery();
			
			/* fww�����޸�,�����ѯ֧��[ },{]��ʽ
			 * FIXME �˴��ȸ�д�����������ѯ����ͨ��Lucene ��û���Ż�֮ǰ������ 
			 * */
			String q = query.getQuery();
			StringBuilder sb = new StringBuilder(q);
			if(rangeQuery != null) {
				for (String rq : rangeQuery) {
					transforRangeToQuery(rq ,sb);
				}
				rangeQuery = null;
				query.setQuery(sb.toString());
				query.remove("rq");
			}

			if (rangeQuery != null) {
				Map<String, Range> rangeMaps = new HashMap<String, Range>();
				for (String rq : rangeQuery) {
					Range r = Range.parse(rq);

					if (r != null) {
						SchemaField sf = null;
						try {
							sf = solrCore.getSchema().getField(r.fn);
						} catch (SolrException e) {
							throw new TerminatorServiceException(e);
						}
						
						FieldType ft = sf.getType();
						if (ft instanceof RangeField) {
							r.ft = ft;
							String targeFn = RangeField.Utils.genRFName(r.fn, ((RangeField) ft).getType());
							Range oldR = rangeMaps.put(targeFn, r);
							if (oldR != null) { // ���ظ���RangeQuery
								throw new TerminatorServiceException("Duplicat RangeQuery ==> {" + oldR.fn + "}");
							}
						} else {
							throw new TerminatorServiceException("The Filed {" + r.fn + "} is not RangeField!");
						}
					}
				}

				RangeThreadLocalContext.getInstance().set(rangeMaps);
				hasRange = true;
			}
			
			String[] sfs = query.getSortFields();
			if(sfs != null) {
				if (sfs.length > 1) {
					throw new TerminatorServiceException("Not support multi-field sort!");
				}
				
				if (sfs.length == 1) {
					SortFieldThreadLocalContext.getInstance().set(sfs[0]);
					query.remove(CommonParams.SORT);
					hasSortField = true;
				}
			} else {
				query.remove("defType");
				query.add("defType","lucene");
			}

			long startTime = System.currentTimeMillis();
			QueryResponse resp = null;
			try {
				resp = solrServer.query(query);
			} catch (Exception e) {
				throw new TerminatorServiceException(e);
			}

			long totalTime = System.currentTimeMillis() - startTime;
			if (totalTime >= 3 * 1000) {
				log.warn("Queryʱ�������ִ�е�Request ==> " + query.toString() + " ִ��ʱ�� ==> " + totalTime);
			} else if (log.isDebugEnabled()) {
				log.debug("ִ�е�Request ==> " + query.toString() + " ִ��ʱ�� ==> " + totalTime);
			}
			return resp;
		} finally {
			if(hasRange) {
				RangeThreadLocalContext.getInstance().remove();
			}
			if(hasSortField) {
				SortFieldThreadLocalContext.getInstance().remove();
			}
		}
	}
	
	public static class Range {
		public FieldType ft;
		public String fn;
		public long start;
		public long end;
		public boolean includeStart;
		public boolean includeEnd;
		
		public Range(String fn, long start, long end, boolean includeStart, boolean includeEnd) {
			super();
			this.fn = fn;
			this.start = start;
			this.end = end;
			this.includeStart = includeStart;
			this.includeEnd = includeEnd;
		}
		
		public boolean inRange(long num) {
			if(num > start && num < end) {
				return true;
			} else if(num == start) {
				return this.includeStart;
			} else if(num == end) {
				return this.includeEnd;
			}
			return false;
		}

		@Override
		public String toString() {
			return "Range [end=" + end + ", fn=" + fn + ", includeEnd=" + includeEnd + ", includeStart=" + includeStart + ", start=" + start + "]";
		}
		
		/**
		 * count:[1 TO 3] ���� count:{1 TO 5]
		 * @param rangeQuery
		 * @return
		 */
		public static Range parse(String rangeQuery) throws TerminatorServiceException{
			String[] parts = rangeQuery.split(":");
			if(parts.length != 2) {
				throwException(rangeQuery,null,null);
			}
			
			String fn = parts[0];
			String range = parts[1];
			
			String[] rps = range.split(" ");
			
			if(rps.length != 3) {
				throwException(rangeQuery,null,null);
			}
			
			String start = rps[0];
			String end   = rps[2];
			
			char startC = start.charAt(0);
			char endC   = end.charAt(end.length() - 1);
			
			if((startC != '[' && startC != '{') || (endC != ']' && endC != '}')) {
				throwException(rangeQuery,null,"Range Char ERROR, {" + startC + "," + endC+"}");
			}
			
			try {
				long startNum = Long.MIN_VALUE;
				long endNum   = Long.MAX_VALUE;
				String startS = start.substring(1).trim();
				String endS   = end.substring(0,end.length()-1).trim();
				
				if(!startS.equals("*")) { 
					startNum = Long.valueOf(startS);
				}

				if(!endS.equals("*")) {
					endNum   = Long.valueOf(endS);
				}
				
				if(startNum > endNum) {
					throwException(rangeQuery,null,"startNum < endNum");
				}
				return new Range(fn, startNum, endNum, startC == '[', endC == ']');
			} catch (Throwable e) {
				if(!(e instanceof TerminatorServiceException)) {
					throwException(rangeQuery, e,"other ERROR");
				}
				throw (TerminatorServiceException)e;
			}
		}
		
		private static void throwException(String rangeQuery,Throwable cause,String msg) throws TerminatorServiceException{
			throw new TerminatorServiceException("rangeQuery ��ʽ������Ҫ�� ==> {" + rangeQuery +"} " + (msg != null?msg:""),cause);
		}
	}
	
	public static class RangeThreadLocalContext {
		private static final ThreadLocal<Map<String,Range>> tl = new ThreadLocal<Map<String,Range>>();
		public static ThreadLocal<Map<String,Range>> getInstance() {
			return tl;
		}
	}
	
	public static class SortFieldThreadLocalContext {
		private static final ThreadLocal<String> tl = new ThreadLocal<String>();
		public static ThreadLocal<String> getInstance() {
			return tl;
		}
	}
	
	
	/**
	 * �����ڷ������Ż�ʱ֧�ְ뿪��ղ�ѯ
	 * @author fuweiwei.pt
	 *
	 */
	public static class Range2 {
		public FieldType ft;
		public String fn;
		public String start;
		public String end;
		public boolean includeStart;
		public boolean includeEnd;
		
		public Range2(String fn, String start, String end, boolean includeStart, boolean includeEnd) {
			super();
			this.fn = fn;
			this.start = start;
			this.end = end;
			this.includeStart = includeStart;
			this.includeEnd = includeEnd;
		}
		
		/*public boolean inRange(long num) {
			if(num > start && num < end) {
				return true;
			} else if(num == start) {
				return this.includeStart;
			} else if(num == end) {
				return this.includeEnd;
			}
			return false;
		}*/

		@Override
		public String toString() {
			return "Range [end=" + end + ", fn=" + fn + ", includeEnd=" + includeEnd + ", includeStart=" + includeStart + ", start=" + start + "]";
		}
		
		/**
		 * count:[1 TO 3] ���� count:{1 TO 5]
		 * @param rangeQuery
		 * @return
		 */
		public static Range2 parse(String rangeQuery) throws TerminatorServiceException{
			String[] parts = rangeQuery.split(":");
			if(parts.length != 2) {
				throwException(rangeQuery,null,null);
			}
			
			String fn = parts[0];
			String range = parts[1];
			
			String[] rps = range.split(" ");
			
			if(rps.length != 3) {
				throwException(rangeQuery,null,null);
			}
			
			String start = rps[0];
			String end   = rps[2];
			
			char startC = start.charAt(0);
			char endC   = end.charAt(end.length() - 1);
			
			if((startC != '[' && startC != '{') || (endC != ']' && endC != '}')) {
				throwException(rangeQuery,null,"Range Char ERROR, {" + startC + "," + endC+"}");
			}
			
			try {
				String startS = start.substring(1).trim();
				String endS   = end.substring(0,end.length()-1).trim();
				
				return new Range2(fn, startS, endS, startC == '[', endC == ']');
			} catch (Throwable e) {
				if(!(e instanceof TerminatorServiceException)) {
					throwException(rangeQuery, e,"other ERROR");
				}
				throw (TerminatorServiceException)e;
			}
		}
		
		private static void throwException(String rangeQuery,Throwable cause,String msg) throws TerminatorServiceException{
			throw new TerminatorServiceException("rangeQuery ��ʽ������Ҫ�� ==> {" + rangeQuery +"} " + (msg != null?msg:""),cause);
		}
	}
	
}