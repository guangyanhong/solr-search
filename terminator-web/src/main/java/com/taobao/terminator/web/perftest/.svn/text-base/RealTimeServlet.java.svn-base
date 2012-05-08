/*package com.taobao.terminator.web.perftest;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.terminator.common.perfutil.PerfTracer;
import com.taobao.terminator.common.protocol.AddDocumentRequest;
import com.taobao.terminator.common.protocol.DeleteByIdRequest;
import com.taobao.terminator.common.protocol.RealTimeService;
import com.taobao.terminator.common.protocol.UpdateDocumentRequest;
import com.taobao.terminator.core.realtime.Bootstraper;
import com.taobao.terminator.core.service.MultiServiceContainer;

public class RealTimeServlet extends HttpServlet{
    Log logger = LogFactory.getLog(RealTimeServlet.class);
	
	private static final long serialVersionUID = 1L;
	private RealTimeDataProducer producer = null;
	private MultiServiceContainer serviceContainer;
	private RealTimeService realTimeService;
	volatile boolean isRunning = false;
	volatile int intervalTime = 1;
	private Thread t;
	
	public static int DEFAULT_INTERVAL_TIME = 1;
	
	@Override
	public void init() throws ServletException {
		try {
			logger.warn("初始化实时数据构造器...");
			RealTimeDataProvider provider = new RealTimeDataProvider();
			provider.setAddRatio(0);
			provider.setUpdateRatio(100);
			provider.setDelRatio(0);
			provider.init();
			producer = new RealTimeDataProducer(provider);
			producer.start();
			
			serviceContainer = MultiServiceContainer.getInstance();
			Bootstraper boostraper = (Bootstraper)serviceContainer.getService("search4ecrmperf-0", "terminatorService");
			realTimeService = boostraper.getRealTimeService();
			final PerfTracer perfTracer = new PerfTracer("RealTime-Request-Producer", logger);
			
			t = new Thread() {
				public void run() {
					while(isRunning) {
						try {
							Thread.sleep(intervalTime);
							Object obj = producer.nextData();
							perfTracer.increment();
							if(obj instanceof AddDocumentRequest) {
								realTimeService.add((AddDocumentRequest)obj);
							} else if(obj instanceof DeleteByIdRequest) {
								realTimeService.delete((DeleteByIdRequest)obj);
							} else if(obj instanceof UpdateDocumentRequest) {
								realTimeService.update((UpdateDocumentRequest)obj);
							}
						}catch (Exception e) {
							logger.error(e,e);
						}
					}
				}
			};
		} catch (Exception e) {
			logger.error("初始化异常",e);
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		
		String action = req.getParameter("action");
		if(action == null || action.trim().equals("")) {
			out.print("action呢?");
			out.flush();
			return ;
		}
		
		if(action.equalsIgnoreCase("start") || action.equalsIgnoreCase("stop")) {
			boolean isStartReq = action.equalsIgnoreCase("start");
			
			if(isStartReq && this.isRunning) {
				out.print("正在运行了，就别老start了,好吧.");
				out.flush();
				return ;
			} else if(isStartReq && !this.isRunning){
				this.intervalTime = this.fetchIntervalTime(req);
				this.isRunning = true;
				t.start();
				out.print("开始跑了，请求间隔时间为 ==>" + intervalTime);
			} else if(!isStartReq && this.isRunning) {
				this.isRunning = false;
			} else {
				out.print("还没有开始运行呢，怎么Stop呢?");
				out.flush();
				return ;
			}
		} 
		
		if(action.equalsIgnoreCase("tune")) {
			this.intervalTime = this.fetchIntervalTime(req);
			out.print("请求间隔时间修改为 ==> " + intervalTime);
			out.flush();
		}
	}
	
	private int fetchIntervalTime(HttpServletRequest req) {
		int intervalTime = DEFAULT_INTERVAL_TIME;
		String intervalTimeStr = req.getParameter("intervalTime");
		if(intervalTimeStr != null && !intervalTimeStr.trim().equals("")) {
			try {
				intervalTime = Integer.valueOf(intervalTimeStr);
			} catch (NumberFormatException e) {
				
			}
		}
		return intervalTime;
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.doGet(req, resp);
	}
}
*/