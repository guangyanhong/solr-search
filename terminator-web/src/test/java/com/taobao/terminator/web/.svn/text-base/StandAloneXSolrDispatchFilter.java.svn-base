package com.taobao.terminator.web;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.solr.servlet.SolrDispatchFilter;

import com.taobao.terminator.core.service.MultiServiceContainer;

public class StandAloneXSolrDispatchFilter extends SolrDispatchFilter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		try {
			//Solr∆Ù∂Ø
			super.init(filterConfig);
			
			//≥ı ºªØTermnatorService
			MultiServiceContainer.createInstance(cores);

		} catch (Throwable e) {
			throw new ServletException("Error! start XSolrDispatchFilter", e);
		}
	}

}
