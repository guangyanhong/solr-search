package com.taobao.terminator.core;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ClassLoaderTest {
	public static void main(String[] args) throws MalformedURLException, ClassNotFoundException {
		ClassLoader parent = Thread.currentThread().getContextClassLoader();
		File file = new File("D:\\data-puller\\lib\\lucene-test-2.5.0.jar");
		URL url = file.toURI().normalize().toURL();
		final ClassLoader cl = URLClassLoader.newInstance(new URL[]{url}, parent);

		Class clazz = cl.loadClass("com.taobao.lucene.Person");
		ApplicationContext context = new ClassPathXmlApplicationContext("spring.xml"){
			@Override
			public ClassLoader getClassLoader() {
				return cl;
			}
		};
	}
}
