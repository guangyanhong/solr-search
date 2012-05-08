package com.taobao.terminator.web;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.core.realtime.Bootstraper3;
import com.taobao.terminator.core.service.MultiServiceContainer;

public class FullDumpServlet extends HttpServlet {
	
	private static Log logger = LogFactory.getLog(FullDumpServlet.class);
	
	public final static String CORE_NAME = "core";
	public final static String DUMP_SERVICE_NAME = "terminatorService";
	
	private static final long serialVersionUID = 8635450997720828371L;
	private MultiServiceContainer serviceContainer;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		this.serviceContainer = MultiServiceContainer.getInstance();
	}
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
						throws ServletException, IOException {
		logger.info("�յ�����===>" + request.getRequestURL().toString());
		
		request.setCharacterEncoding("UTF-8");
		response.setContentType("text/plain; charset=GBK");
		PrintWriter out = response.getWriter();
		
		String coreName = request.getParameter(CORE_NAME);
		if(StringUtils.isBlank(coreName)) {
			out.println("��������������core����");
			out.flush();
			out.close();
			return;
		}
		
		Bootstraper3 bootstraper = null;
		try {
			Object objService = serviceContainer.getService(coreName, DUMP_SERVICE_NAME);
			if(objService instanceof Bootstraper3) {
				bootstraper = (Bootstraper3)objService;
			}
		} catch(Exception e) {
			logger.error("��ȡָ����Serviceʧ��===>" + coreName);
			out.println("��ȡָ����Serviceʧ��");
			out.flush();
			out.close();
			return;
		}
		
		if(bootstraper != null) {
			try {
				if(bootstraper.isLeader()) {
					bootstraper.getLeaderContainer().fullDump();
					out.println("����ȫ������ɹ�");
				} else {
					out.print("�������̨������Follower,ȫ��Dump��Leader�ܣ�");
				}
			} catch (Exception e) {
				logger.error("����ȫ������ʧ��===>" + coreName, e);
				out.println("����ȫ������ʧ��");
			}
		}
		out.flush();
		out.close();
	}
}