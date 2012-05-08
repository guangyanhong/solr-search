package com.taobao.terminator.web.perftest;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.AddUpdateCommand;

import com.taobao.terminator.client.realtime.TerminatorBean;
import com.taobao.terminator.common.TerminatorServiceException;
import com.taobao.terminator.common.protocol.TerminatorQueryRequest;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;

public class IpFriendServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;
	private  final String SOLR_URL = "http://10.232.22.129:8080/terminator-search/search4prehandle-0";
	Log logger = LogFactory.getLog(IpFriendServlet.class);
	
	private CommonsHttpSolrServer solrServer ;
	public final  String TYPE = "type";
	public final  String PARAM = "param";
	private TerminatorBean bean ; 
	volatile long start;
	private Random random;
	private DecimalFormat dateFormat;
	
	@Override
	public void init() throws ServletException {
		try {
		super.init();
		//初始化查询信息
		start = System.currentTimeMillis();
		this.getServletContext().setAttribute("start", start);
		this.bean = new TerminatorBean();
		bean.setServiceName("search4allenhandle");
		bean.setZkAddress("10.232.15.46:2181");
		bean.init();
		dateFormat = new DecimalFormat("00");
		this.solrServer = new CommonsHttpSolrServer(this.SOLR_URL);
		} catch (Exception e) {
			throw new RuntimeException("ipServlet初始化异常", e);
		}
		random = new Random();
		random.setSeed(random.nextLong() ^ System.currentTimeMillis() ^ hashCode() ^ Runtime.getRuntime().freeMemory());
	}
	
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//根据传入参数进行查询
		logger.info("收到请求===>" + request.getRequestURL().toString());
		request.setCharacterEncoding("UTF-8");
		response.setContentType("text/plain; charset=GBK");

		String type = request.getParameter(TYPE);
		String para = request.getParameter(PARAM);
		if(!StringUtils.isBlank(type) && !StringUtils.isBlank(para)) {
			if("common".equals(type)) {
				try {
					doCommon(para);
				} catch(Exception e) {
					throw new RuntimeException("solr提交更新失败", e);
				} 
			} else if("realtime".equals(type)){
				try {
					doRealtime(para);
				} catch (Exception e) {
					throw new RuntimeException("solr实时提交更新失败", e);
				}
			} 
//			maybeCommit(this.solrServer);
		} else {
			throw new RuntimeException("请确认输入的查询条件不为空");
		}
	}
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		 this.doGet(req, resp);
	}
	
	//同时搜索和更新文档，四种情况  0|表示无搜索只有更新  1|表示搜索，更新1:1  2|表示搜索，更新2:1  3|表示满意搜索->更新 3:1
	//common搜索
	protected void doCommon(String para) throws Exception{
		int paraType = Integer.parseInt(para);
		//构造一个更新条件
		this.commonUpdate();
		//构造para个搜索条件
		for(int i = 0; i<paraType; i++) {
		    doQuery();
		}
	}
	
	
	protected void maybeCommit(SolrServer solrServer) {
		long end = System.currentTimeMillis();
		long checktime = (Long)this.getServletContext().getAttribute("start");
		if((end - checktime)/1000.0 > 1) {
			logger.warn("当前commit时间：" + end);
			this.getServletContext().setAttribute("start", end);
			try {
				solrServer.commit();
			} catch (Exception e) {
				logger.error("更新提交索引失败", e);
			}
			
		}
	}
	
	//realtime搜索
	protected void doRealtime(String para) throws Exception{
		//同时搜索和更新文档
		int paraType = Integer.parseInt(para);
		//构造一个更新条件
		this.realtimeUpdate();
		//构造para个搜索条件
		for(int i = 0; i<paraType; i++) {
		    doQuery();
		}
	}
	
	//common搜索实现
	protected void  commonUpdate() throws SolrServerException, IOException {
		//doQuery();
		SolrInputDocument doc = generateDocument();
		this.solrServer.add(doc);
	}
	//realtime搜索
	protected void realtimeUpdate() throws TerminatorServiceException{
		UpdateDocumentRequest updateReq = new UpdateDocumentRequest();
		SolrInputDocument solrDoc = generateDocument();
		updateReq.solrDoc = solrDoc;
		bean.update(updateReq);
	}
	
	//构造更新条件
	private SolrInputDocument generateDocument() {
		int id = this.random.nextInt(40000000);
		//select一次
		if(id > 0) {
			SolrInputDocument doc = new SolrInputDocument();
			String idString = id + ""; // 1,2,3,4....
			doc.addField("id", idString); // 1,2,3,4....
			doc.addField("s_id", random.nextInt(200000) + ""); // 100000001,100000002,100000003,100000004....
			doc.addField("b_id", random.nextInt(1000000) + ""); // 1,2,3,4....
			doc.addField("level", random.nextInt(10) + ""); // 1,2,3,4,5,6....,9
			doc.addField("status", random.nextBoolean() ? "1" : "-3"); // 1|-3
			doc.addField("sex", random.nextInt(2) + ""); // 0,1
			doc.addField("pro", random.nextInt(36) + ""); // 0-36
			doc.addField("city", "杭州"); // 杭州
			doc.addField("source", random.nextBoolean() ? "1" : "2"); // 1,2
			doc.addField("group", "1|2|3|4|6|7|8");
			int count = random.nextInt(300) + 1;
			int am = random.nextInt(20000) + 300;
			doc.addField("count", count + ""); // 1-300
			doc.addField("am", am + ""); // 20000-20300
			// l_t : 20001229-20111229
			doc.addField("l_t","20" + dateFormat.format(random.nextInt(11))+ dateFormat.format(random.nextInt(12) + 1)+ dateFormat.format(random.nextInt(30 + 1)));
			doc.addField("bir", dateFormat.format(random.nextInt(13)) + dateFormat.format(random.nextInt(31)));
			doc.addField("i_c", 1000 + ""); // 1000
			doc.addField("a_p", (am / count) + "");
			doc.addField("c_n", random.nextInt(100) + ""); // 100
			doc.addField("n_c", count + random.nextInt(count) + "");
			doc.addField("n_a", 100 + "");
			doc.addField("g_m", "20110517154846");
			doc.addField("nick", "nick_" + id); // nick_1
			return doc;
			}
		return null;
	}
	
	//构造查询条件
	private String generateQuery() {
		long s_id = random.nextInt(200000); 
		String start = "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1));
		String end = "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1));
	    if(start.compareTo(end) > 0) {
	    	String middle = end;
	    	end = start ;
	    	start = middle;
	    } 
	    int level = random.nextInt(10) + 1 ;
	    
	    StringBuffer buffer = new StringBuffer();
	    buffer.append("s_id:" + s_id);
	    buffer.append(" AND ");
	    buffer.append("level:" + level);
	    /*buffer.append(" AND ");
	    buffer.append("pro:" + pro);
	    buffer.append(" AND ");
	    buffer.append("l_t:[" + start + " TO " + end + "]" );*/
	 
		return buffer.toString();
	}
	
	
	private void doQuery() throws Exception{
		TerminatorQueryRequest q = new TerminatorQueryRequest();
		q.setStart(0);
		q.setRows(10);
		q.setQuery(generateQuery());
		this.bean.query(q);
	}
	
	public static void main(String[] args) {
		Random random = new Random();
		DecimalFormat dateFormat = new DecimalFormat("00");
		random.setSeed(random.nextLong() ^ System.currentTimeMillis()  ^ Runtime.getRuntime().freeMemory());
		long s_id = random.nextInt(200000); 
		String start = "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1));
		String end = "20" + dateFormat.format(random.nextInt(11)) +dateFormat.format(random.nextInt(12) + 1) + dateFormat.format(random.nextInt(30 + 1));
	    if(start.compareTo(end) < 0) {
	    	String middle = end;
	    	end = start ;
	    	start = middle;
	    } 
	    int level = random.nextInt(10) + 1 ;
	    String pro = random.nextInt(36) + "";
	    
	    StringBuffer buffer = new StringBuffer();
	    buffer.append("s_id:" + s_id);
	    buffer.append(" AND ");
	    buffer.append("level:" + level);
	    buffer.append(" AND ");
	    buffer.append("pro:" + pro);
	    buffer.append(" AND ");
	    buffer.append("l_t:[" + start + " TO " + end + "]" );
	 
	    System.out.println(buffer.toString());
	}
}
