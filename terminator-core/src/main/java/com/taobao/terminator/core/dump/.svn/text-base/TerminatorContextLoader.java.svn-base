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
 * 本类负责动态加载Spring的配置文件
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
	 * @param paths Spring配置文件的绝对路径
	 */
	public TerminatorContextLoader(String templateBasePath, List<String> configNames, Map<String, Object> values, ClassLoader classLoader) {
		this.tempalteBasePath	= templateBasePath;
		this.configNames		= configNames;
		this.classLoader 		= classLoader;
		this.isInited 			= false;
		this.values 			= values;
	}
	
	/**
	 * 初始化方法
	 * @throws FileNotFoundException 指定的配置文件不存在
	 */
	public void init() throws FileNotFoundException {
		this.velocityInit();
		
		List<String> parsedXmlList = new ArrayList<String>();
		
		for(String configName: configNames) {
			
			if(!(new File(this.tempalteBasePath+File.separator+configName).exists())) {
				log.error(configName+"文件不存在，加载失败");
				throw new FileNotFoundException("指定的applicationContext.xml文件不存在 ==> " + configName);
			}
			
			String parsedString = this.parseSpringConfig(configName);
			if(parsedString == null) {
				throw new InitException("Spring配置文件解析合并失败,文件名 ==> " + configName);
			} else {
				parsedXmlList.add(parsedString);
			}
		}
		
		if(parsedXmlList.size() == 0) {
			log.error("没有任何配置文件解析成功！！！");
			throw new InitException("Spring配置文件均没有没有解析合并成功");
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
	 * 初始化velocity模板引擎
	 */
	protected void velocityInit() {
		this.engine = new VelocityEngine();
		Properties properties = new Properties(); 
		properties.setProperty(Velocity.FILE_RESOURCE_LOADER_PATH, this.tempalteBasePath);
		properties.setProperty(Velocity.FILE_RESOURCE_LOADER_CACHE, "true");
		try {
			engine.init(properties);
		} catch (Exception e) {
			log.error("velocity引擎初始化失败", e);
		}
	}
	
	/**
	 * 读入模板，并作简单替换
	 * @param path applicationContext.xml路径
	 * @return 文本内容
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
			log.error("指定的模板找不到："+templateName, e);
		} catch (ParseErrorException e) {
			log.error("模板解析错误:"+templateName, e);
		} catch (Exception e) {
			log.error("模板解析失败:"+templateName, e);
		}
		return null;
	}
}
