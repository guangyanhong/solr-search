package com.taobao.terminator.web;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.taobao.terminator.web.mock.FilterConfigMock;
import com.taobao.terminator.web.mock.HttpServletRequestMock;
import com.taobao.terminator.web.mock.HttpServletResponseMock;
import com.taobao.terminator.web.mock.ServletConfigMock;

public class FullDumpServletTest {

	private static void initFilter() throws ServletException {
		StandAloneXSolrDispatchFilter xSolrDispatchFilter = new StandAloneXSolrDispatchFilter();
		FilterConfig filterConfig = new FilterConfigMock();
		xSolrDispatchFilter.init(filterConfig);
	}
	
	private static FullDumpServlet initServlet() throws ServletException {
		FullDumpServlet fullDumpServlet = new FullDumpServlet();
		ServletConfigMock servletConfigMock = new ServletConfigMock();
		fullDumpServlet.init(servletConfigMock);
		return fullDumpServlet;
	}
	
	private static HttpServletRequest makeRequest() {
		HttpServletRequestMock request = new HttpServletRequestMock();
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("core", "search4widget");
		request.setParameters(parameters);
		return request;
	}
	
	/**
	 * @param args
	 * @throws ServletException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws ServletException, IOException, InterruptedException {
		System.setProperty("solr.solr.home", "D:\\teminator_home");
		initFilter();
		FullDumpServlet fullDumpServlet = initServlet();
		HttpServletRequest request = makeRequest();
		HttpServletResponseMock response = new HttpServletResponseMock();
		fullDumpServlet.doGet(request, response);
		
		Thread.sleep(900000000L);
	}

}
