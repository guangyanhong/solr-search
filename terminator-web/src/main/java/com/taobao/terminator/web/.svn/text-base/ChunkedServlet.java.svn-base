package com.taobao.terminator.web;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.xml.serialize.Printer;

public class ChunkedServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//		super.doGet(req, resp);
//		resp.setHeader("Transfer-Encoding", "chunked");
//		DataOutputStream out = new DataOutputStream(resp.getOutputStream());
		resp.setHeader("Pragma","No-cache"); 
		resp.setHeader("Cache-Control","no-cache");
		resp.setDateHeader("Expires", 0);  
		PrintWriter out =  resp.getWriter();
		
		StringBuffer sb = new StringBuffer();
		for(int i =0;i<3;i++) {
			sb.append(i);
		}
		
		String content = sb.toString();
		for(int i = 0;i<100;i++) {
			
//			String l = Integer.toHexString(content.getBytes().length);
			
//			out.print(l);
//			out.print("\r\n");
			out.print(content);
//			out.print("\r\n");
			out.flush();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		System.out.println(Integer.toHexString("1".getBytes().length));
	}
}
