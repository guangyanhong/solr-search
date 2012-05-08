package org.taobao.terminator.client.test;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.taobao.terminator.client.index.data.MultiTableDataProvider;

public class MultiTableDataProviderTest {
	public static void main(String[] args) throws Exception {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setPassword("tag");
		ds.setUsername("tag");
		ds.setUrl("jdbc:mysql://192.168.205.101:3306/tag?characterEncoding=GBK");
		String baseSql = "SELECT" +
							" id ,product_id as productId ,product_type as productType,entity_type as entityType ,entity_tags as tags,entity_url as entityUrl , product_create" + 
						" FROM" + 
							" tag_entity$tablename$" +
						" WHERE" + 
							" gmt_modified<'$lastModified$'" + 
						" AND" +
							" product_create is not null" +
						" AND" +
							" gmt_modified<'$now$'";
		MultiTableDataProvider mp = new MultiTableDataProvider(ds,baseSql,"");
		mp.setSubtableDesc("0-1");
		mp.init();
		
		while(mp.hasNext()){
			System.out.println(mp.next());
			System.out.println("====> " + mp.getTotalFetchedNum());
		}
	}
}	
