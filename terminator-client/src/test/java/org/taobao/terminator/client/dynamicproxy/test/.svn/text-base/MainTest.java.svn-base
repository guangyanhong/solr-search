package org.taobao.terminator.client.dynamicproxy.test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class MainTest {
	public interface Service{
		public String getName();
	}
	public static void main(String[] args) throws Exception {
		InvocationHandler h = new InvocationHandler(){

			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				System.out.println("hello this is proxy");
				return null;
			}
		};
		Service s = (Service)Proxy.newProxyInstance(MainTest.class.getClassLoader(),new Class[]{Service.class}, h);
		s.getName();
	}
}
