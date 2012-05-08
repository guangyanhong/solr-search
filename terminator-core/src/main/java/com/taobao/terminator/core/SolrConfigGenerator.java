package com.taobao.terminator.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.core.SolrResourceLoader;
import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

/**
 * solr.xml文件生成器<br><br>
 * 在服务启动阶段使用<br>
 * 如果没有solr.xml文件，则动态创建<br>
 * 如果有相应的solr.xml文件，则保留原有的配置，根据新的配置对原有的配置进行修改(新增/删除core节点，原有节点如果没有增减，则置之不理)
 * 
 * @author yusen
 *
 */
public class SolrConfigGenerator {
	public static final String SOLR_XML_NAME = "solr.xml";
	private String solrHome = SolrResourceLoader.locateSolrHome();
	
	public SolrConfigGenerator(){}
	public SolrConfigGenerator(String solrHome){
		if(solrHome != null)
			this.solrHome = solrHome;
	}
	
	public void generateSolrXml() throws JDOMException,IOException{
		File file =  new File(solrHome,SOLR_XML_NAME);
		if(file.exists()){
			updateFile(file);
		}else{
			createAndInitFile(file);
		}
	}
	
	private File[] listCoreFiles(File solrHomeDir){
		return  solrHomeDir.listFiles(new java.io.FileFilter(){
			public boolean accept(File file) {
				if(file.isFile()) 
					return false;
				String fileName = file.getName();
				String[] s = fileName.split("-");
				if(s.length <= 1 ) 
					return false;
				return true;
			}
		});
		
	}
	
	public void createAndInitFile(File solrFile) throws IOException{
			
		File solrHomeDir = solrFile.getParentFile();
		File[] solrCoreFileList = this.listCoreFiles(solrHomeDir);
		SolrConfigTemplate template = new SolrConfigTemplate();
		for(File coreFile : solrCoreFileList){
			String fileName = coreFile.getName();
			template.appendCore(fileName, fileName);
		}
		String content = template.getConfig();
		Writer w = null;
		try{
			w=new FileWriter(solrFile);
			w.write(content);
			w.flush();
		}finally{
			w.close();
		}
	}
	
	@SuppressWarnings("unchecked")
	public  void updateFile(File solrFile) throws JDOMException, IOException{
		SAXBuilder saxBuilder=new SAXBuilder();
		InputStream input = new FileInputStream(solrFile);
		Document doc = saxBuilder.build(input);
		Element rootEle = doc.getRootElement();
		Element coresEle = rootEle.getChild("cores");
		List<Element> coreEles = coresEle.getChildren("core");
		List<String> oldCoreNames = new ArrayList(coreEles.size());
		for(Element ce : coreEles){
			oldCoreNames.add(ce.getAttributeValue("name"));
		}
		
		File solrHomeDir = solrFile.getParentFile();
		File[] solrCoreFileList = this.listCoreFiles(solrHomeDir);
		
		final List<String> newCoreNames = new ArrayList<String>();
		for(File solrCoreFile : solrCoreFileList){
			String coreName = solrCoreFile.getName();
			newCoreNames.add(coreName);
			if(!oldCoreNames.contains(coreName)){
				Element coreEle = new Element("core");
				Attribute nameAtt = new Attribute("name" ,solrCoreFile.getName());
				Attribute instanceDirAtt = new Attribute("instanceDir" ,solrCoreFile.getName());
				coreEle.setAttribute(nameAtt);
				coreEle.setAttribute(instanceDirAtt);
				coresEle.addContent(coreEle);
			}
		}
		
		Iterator<Element> it = coreEles.iterator();
		while(it.hasNext()){
			Element coreEle = it.next();
			final String name = coreEle.getAttributeValue("name");
			if(!newCoreNames.contains(name)){
				it.remove();
			}
		}
		
		XMLOutputter out = new XMLOutputter();
		String docString = out.outputString(doc);
		FileWriter fw = null;
		try{
			
			fw = new FileWriter(solrFile);
			fw.write(docString);
			fw.flush();
		}finally{
			fw.close();
		}
	}
	
	/**
	 * solr.xml模板生成类
	 * 
	 * @author yusen
	 */
	class SolrConfigTemplate{
		public static final String CORE_TEMPLATE = "<core name=\"{0}\" instanceDir=\"{1}\" dataDir=\"data\"/>";
		
		private StringBuilder sb = null;
		private boolean isGenerated = false;
		
		public SolrConfigTemplate(){
			sb = new StringBuilder();
			sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?> \n")
			.append("<solr persistent=\"true\" sharedLib=\"lib\"> \n <cores adminPath=\"/admin/cores\">\n");
		}

		public StringBuilder appendCore(String name, String instanceDir) {
			Object arguments[] = {name, instanceDir};
			String item = MessageFormat.format(CORE_TEMPLATE, arguments);
			sb.append(item).append("\n");
			return sb;
		}
		
		public String getConfig(){
			if(!isGenerated){
				sb.append("</cores> \n  </solr> \n");
				this.isGenerated = true;
			}
			return sb.toString();
		}
	}
	
	public static void main(String[] args) throws JDOMException, IOException {
		SolrConfigGenerator gen = new SolrConfigGenerator("D:\\all-nodes\\terminator-nodes\\192.168.222.33");
		gen.generateSolrXml();
	}
}	
