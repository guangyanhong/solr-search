package com.taobao.terminator.web.mock;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

public class ServletConfigMock implements ServletConfig {

	private ServletContextMock servletContextMock = new ServletContextMock();
	
	private Map<String, String> parameters = new HashMap<String, String>();
	
	@Override
	public String getInitParameter(String arg0) {
		return parameters.get(arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Enumeration getInitParameterNames() {
		return (Enumeration)Collections.enumeration(parameters.keySet());
	}
	
	@Override
	public ServletContext getServletContext() {
		return servletContextMock;
	}

	@Override
	public String getServletName() {
		return "fullDumpServlet";
	}

}
