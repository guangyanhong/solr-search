package org.taobao.terminator.client.test;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.taobao.terminator.client.index.data.DataProviderException;
import com.taobao.terminator.client.index.data.SingleTableDataProvider;

public class SingleTableDataProviderTest {
	public static void main(String[] args) throws DataProviderException {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setPassword("tag");
		ds.setUsername("tag");
		ds.setUrl("jdbc:mysql://192.168.205.101:3306/tag?characterEncoding=GBK");
		String sql = "select" +
					" id ,product_id as productId ,product_type as productType,entity_type as entityType ,entity_tags as tags,entity_url as entityUrl , product_create" + 
				" from" + 
					" tag_entity_0001" +
				" where" + 
					" gmt_modified<'$lastModified$'" + 
				" and" +
					" product_create is not null" +
				" and" +
					" gmt_modified<'$now$'";
		SingleTableDataProvider p = new SingleTableDataProvider(ds, sql,"");
		while(p.hasNext()){
			System.out.println(p.next());
		}
	}
}
