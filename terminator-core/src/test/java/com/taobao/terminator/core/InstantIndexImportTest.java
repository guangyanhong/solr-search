package com.taobao.terminator.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.core.CoreContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.xml.sax.SAXException;

import com.taobao.terminator.core.index.importer.InstantSolrXMLDataImporter;

public class InstantIndexImportTest {
	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException{
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		SolrXmlDocGenerator xmlGenerator = new SolrXmlDocGenerator();
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://192.168.205.89:3306/snsfriend?characterEncoding=GBK");
		dataSource.setUsername("snsfriend");
		dataSource.setPassword("snsfriend");
		
 		Connection con = dataSource.getConnection();
		Statement statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		
		
		//JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
		//List<Map> result = jdbcTemplate.queryForList("select * from sns_my_fans_count_0000");
		statement.execute("select * from sns_my_fans_count_0000");
		
		ResultSet result = statement.getResultSet();
		
		ResultSetMetaData metaData = result.getMetaData();
		
		while(result.next()){
			for(int i = 1; i <= metaData.getColumnCount(); i++){
				switch(metaData.getColumnType(i)){
				case Types.DATE:
				case Types.TIMESTAMP:
					System.out.print(result.getDate(i));
					System.out.print(" ");
					System.out.println(result.getTime(i));
				}
			}
		}
		/*for(int i = 0; i < result.size(); i++){
			Map aRow = result.get(i);
			String xml = "";
			try {
				xml = xmlGenerator.mapToSolrXML(aRow, true);
			} catch (Exception e) {
				System.out.println("转换一行出错，忽略这一行。data:" + aRow.toString());
			}
			
			try {
				byteOut.write(xml.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}*/
		
		/*byte[] indexData = byteOut.toByteArray();
		
		//启动solr core
		CoreContainer cores;
		System.setProperty("solr.solr.home", "E:\\taobao_workspace\\solr_server\\");
		CoreContainer.Initializer init = new CoreContainer.Initializer();
		cores = init.initialize();
		
		InstantSolrXMLDataImporter dataImporter = new InstantSolrXMLDataImporter(cores.getCore(""), indexData);
		
		dataImporter.importIndex();*/
	}
}
