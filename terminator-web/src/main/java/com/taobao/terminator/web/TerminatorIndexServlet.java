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
import org.quartz.SchedulerException;

import com.taobao.terminator.core.dump.DefaultDumpService;
import com.taobao.terminator.core.service.MultiServiceContainer;

public class TerminatorIndexServlet extends HttpServlet {
	
	private static Log logger = LogFactory.getLog(TerminatorIndexServlet.class);
	
	public final static String CORE_NAME = "core";
	public final static String INDEX_TYPE = "type";
	public final static String FULL_JOB = "full";
	public final static String INCR_JOB = "incr";
	public final static String DUMP_SERVICE_NAME = "dumpService";
	
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
		String indexType = request.getParameter(INDEX_TYPE);
		if(StringUtils.isBlank(coreName) || StringUtils.isBlank(indexType)) {
			out.println("��������������core��type��������");
			out.flush();
			out.close();
			return;
		}
		
		DefaultDumpService dumpService = null;
		try {
			Object objService = serviceContainer.getService(coreName, DUMP_SERVICE_NAME);
			if(objService instanceof DefaultDumpService) {
				dumpService = (DefaultDumpService)objService;
			}
		} catch(Exception e) {
			logger.error("��ȡָ����Serviceʧ��===>" + coreName);
			out.println("��ȡָ����Serviceʧ��");
			out.flush();
			out.close();
			return;
		}
		
		if(dumpService != null) {
			if(FULL_JOB.equals(indexType)) {
				try {
					dumpService.triggerFullIndexJob();
					out.println("����ȫ������ɹ�");
				} catch (SchedulerException e) {
					logger.error("����ȫ������ʧ��===>" + coreName, e);
					out.println("����ȫ������ʧ��");
				}
			} else if(INCR_JOB.equals(indexType)) {
				try {
					dumpService.triggerIncrIndexJob();
					out.println("������������ɹ�");
				} catch (SchedulerException e) {
					logger.error("������������ʧ��===>" + coreName, e);
					out.println("������������ʧ��");
				}
			} else {
				out.print("ʱ���������ʹ���,�������ʧ��");
			}
		}
		out.flush();
		out.close();
	}
}
