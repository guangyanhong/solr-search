package com.taobao.terminator.web;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.update.UpdateHandler;

import com.taobao.terminator.core.realtime.Bootstraper3;
import com.taobao.terminator.core.realtime.RealTimeIndexReaderFactory;
import com.taobao.terminator.core.realtime.RealTimeUpdateHandler;
import com.taobao.terminator.core.realtime.TerminatorUpdateHandler;
import com.taobao.terminator.core.service.MultiServiceContainer;

public class RealTimeStatsServlet extends HttpServlet {
	private static final long serialVersionUID = -3358279678575460114L;
	private MultiServiceContainer serviceContainer;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		this.serviceContainer = MultiServiceContainer.getInstance();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		
		String coreName =req.getParameter("core");
		String type = req.getParameter("type");
		
		if(StringUtils.isBlank(coreName) || StringUtils.isBlank(type)) {
			out.println("coreName & type can not be blank.");
			out.flush();
			out.close();
			return;
		}
		
		if(type.equalsIgnoreCase("w")) {
			out.print(this.getUpdateHandler(coreName).getStatistics());
			out.flush();
			out.close();
		} else if(type.equalsIgnoreCase("r")){
			out.print(this.getIndexReaderFactory(coreName).getStatistics());
			out.flush();
			out.close();
		} else {
			out.print("type error.");
			out.flush();
			out.close();
			return;
		}
	}
	
	private RealTimeUpdateHandler getUpdateHandler(String coreName) {
		Bootstraper3 bootstraper = this.getBoostraper(coreName);
		
		TerminatorUpdateHandler tuh = (TerminatorUpdateHandler)(bootstraper.getSolrCore().getUpdateHandler());
		UpdateHandler uh = tuh.getProperUpdateHandler();
		if(uh instanceof RealTimeUpdateHandler) {
			return (RealTimeUpdateHandler)uh;
		}
		return null;
	}
	
	private RealTimeIndexReaderFactory getIndexReaderFactory(String coreName) {
		Bootstraper3 bootstraper = this.getBoostraper(coreName);
		IndexReaderFactory irf = bootstraper.getSolrCore().getIndexReaderFactory();
		if(irf instanceof RealTimeIndexReaderFactory) {
			return (RealTimeIndexReaderFactory)irf;
		}
		return null;
	}
	
	private Bootstraper3 getBoostraper(String coreName) {
		Object obj = this.serviceContainer.getService(coreName, "terminatorService");
		return (Bootstraper3)obj;
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.doGet(req, resp);
	}
}
