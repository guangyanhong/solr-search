package com.taobao.terminator.core.dump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.springframework.context.ApplicationContext;

/**
 * ���ฺ��̬����Spring�������ļ�
 */
public class TerminatorContextLoader {
	
	protected static Log log = LogFactory.getLog(TerminatorContextLoader.class);
	
	protected String tempalteBasePath;
	protected List<String> configNames;
	protected ApplicationContext applicationContext;
	protected ClassLoader classLoader;
	protected boolean isInited;
	protected Map<String, Object> values;
	
	protected VelocityEngine engine;
	
	public static final String SPRING_CONFIGS = "springConfigs";
	
	public TerminatorContextLoader() {
		this.isInited = false;
	}
	
	/**
	 * @param paths Spring�����ļ��ľ���·��
	 */
	public TerminatorContextLoader(String templateBasePath, List<String> configNames, Map<String, Object> values, ClassLoader classLoader) {
		this.tempalteBasePath	= templateBasePath;
		this.configNames		= configNames;
		this.classLoader 		= classLoader;
		this.isInited 			= false;
		this.values 			= values;
	}
	
	/**
	 * ��ʼ������
	 * @throws FileNotFoundException ָ���������ļ�������
	 */
	public void init() throws FileNotFoundException {
		this.velocityInit();
		
		List<String> parsedXmlList = new ArrayList<String>();
		
		for(String configName: configNames) {
			
			if(!(new File(this.tempalteBasePath+File.separator+configName).exists())) {
				log.error(configName+"�ļ������ڣ�����ʧ��");
				throw new FileNotFoundException("ָ����applicationContext.xml�ļ������� ==> " + configName);
			}
			
			String parsedString = this.parseSpringConfig(configName);
			if(parsedString == null) {
				throw new InitException("Spring�����ļ������ϲ�ʧ��,�ļ��� ==> " + configName);
			} else {
				parsedXmlList.add(parsedString);
			}
		}
		
		if(parsedXmlList.size() == 0) {
			log.error("û���κ������ļ������ɹ�������");
			throw new InitException("Spring�����ļ���û��û�н����ϲ��ɹ�");
		}
		
		this.applicationContext = new StringXmlApplicationContext(parsedXmlList.toArray(new String[parsedXmlList.size()])) {
			@Override
			public ClassLoader getClassLoader() {
				return TerminatorContextLoader.this.classLoader;
			}
		};
		this.isInited = true;
	}

	public String getTempalteBasePath() {
		return tempalteBasePath;
	}

	public void setTempalteBasePath(String tempalteBasePath) {
		this.tempalteBasePath = tempalteBasePath;
	}

	public List<String> getConfigNames() {
		return configNames;
	}

	public void setConfigNames(List<String> configNames) {
		this.configNames = configNames;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public boolean isInited() {
		return isInited;
	}
	
	/**
	 * ��ʼ��velocityģ������
	 */
	protected void velocityInit() {
		this.engine = new VelocityEngine();
		Properties properties = new Properties(); 
		properties.setProperty(Velocity.FILE_RESOURCE_LOADER_PATH, this.tempalteBasePath);
		properties.setProperty(Velocity.FILE_RESOURCE_LOADER_CACHE, "true");
		try {
			engine.init(properties);
		} catch (Exception e) {
			log.error("velocity�����ʼ��ʧ��", e);
		}
	}
	
	/**
	 * ����ģ�壬�������滻
	 * @param path applicationContext.xml·��
	 * @return �ı�����
	 */
	protected String parseSpringConfig(String templateName) {
		try {
			Template template = engine.getTemplate(templateName);
			VelocityContext context = new VelocityContext();
			for(Entry<String, Object> entry: this.values.entrySet()) {
				context.put(entry.getKey(), entry.getValue());
			}
			StringWriter writer = new StringWriter();
			template.merge(context, writer);
			return writer.toString();
 		} catch (ResourceNotFoundException e) {
			log.error("ָ����ģ���Ҳ�����"+templateName, e);
		} catch (ParseErrorException e) {
			log.error("ģ���������:"+templateName, e);
		} catch (Exception e) {
			log.error("ģ�����ʧ��:"+templateName, e);
		}
		return null;
	}
}
