package com.taobao.terminator.client.index.data;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * ���Oracle����ʹ��Ӳ����SQL�ĸĽ�������PrepareStatement�ķ�ʽ
 * 
 * 2010-11-15 19:23
 * 
 * @author yusen
 */
public class PrepareSingleTableDataProvider extends SingleTableDataProvider{
	protected PreparedStatement prestmt = null;
	
	public void init()throws DataProviderException{
		if(logger.isDebugEnabled()){
			logger.debug("ִ��Sql��ѯ.");
		}
		
		if(isInited())
			return;
		
		if(dataSource == null || StringUtils.isBlank(sql)){
			throw new DataProviderException("[single-init-error] DataSource �� sql���Բ���Ϊnull���߿�,����ע������������.");
		}
		
		if(sqlFuncs == null){
			sqlFuncs = new SqlFunctionCollectors(serviceName);
			sqlFuncs.initDefaultFunctions();
			if(plusSqlFuncs != null && !plusSqlFuncs.isEmpty()){
				logger.warn("�û��Զ�����SqlFunctions");
				for(SqlFunction f : plusSqlFuncs){
					if(sqlFuncs.register(f) != null){
						logger.warn("�û��Զ����SqlFunction ==> " + f.getPlaceHolderName() + " ������Ĭ�ϵ�Function.");
					}
				}
			}
		}
		
		try{
			this.connnection = dataSource.getConnection();
			
			List<String> funcsAndSql = this.parseSql(sql);
			String targetSql = funcsAndSql.get(funcsAndSql.size() - 1);
			if(funcsAndSql.size() >= 2) {
				prestmt = connnection.prepareStatement(targetSql);
				this.buildPrepareStatement(prestmt, funcsAndSql.subList(0, funcsAndSql.size() -1));
			}
			
			logger.warn("ִ�е�SQL == > \n" + targetSql);
			
			this.beforExecuteSql(connnection, statement);
			resultSet  = prestmt.executeQuery();
			metaData   = resultSet.getMetaData();
			columCount = metaData.getColumnCount();
		}catch(Exception e){
			throw new DataProviderException("[single-init-error]",e);
		}
		isInited.set(true);
	}
	
	private void buildPrepareStatement(PreparedStatement prestmt,List<String> funcs) throws SQLException {
		for(int i = 0; i < funcs.size(); i++) {
			String funName = funcs.get(i);
			String funValue = null;
			prestmt.setString(i+1, funValue = sqlFuncs.getValue(funName));
			logger.warn("SQL-FUNCTION ===> " + funName + ":" + funValue);
		}
	}
	
	private List<String> parseSql(String sql) {
		char[] chars = sql.toCharArray();
		boolean start = false;
		int startPos = 0;
		int endPos = 0;
		
		List<String> list = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i<chars.length; i++) {
			if(start && chars[i] == '$') {
				endPos = i;
				start = false;
				list.add(sql.substring(startPos+1, endPos));
				sb.append('?');
				continue;
			}
			if(chars[i] == '$') {
				startPos = i;
				start = true;
			}
			
			if(chars[i] != '$' && !start) {
				sb.append(chars[i]);
			}
		}
		list.add(sb.toString());
		return list;
	}
	
	public static void main(String[] args) {
		PrepareSingleTableDataProvider p = new PrepareSingleTableDataProvider();
		List<String> targetSql = p.parseSql("select * from table_name where gmt_modified>$lastModified$");
		System.out.println(targetSql);
	}
	
	@Override
	public void close() throws DataProviderException {
		logger.warn("�ر�DataProvider,�ͷ����ݿ�������Դ��״̬��־��λ.");
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				prestmt.close();
			if(connnection != null)
				connnection.close();
		} catch (SQLException e) {
			throw new DataProviderException("single-close-error",e);
		} finally {
			resultSet   = null;
			prestmt     = null;
			connnection = null;
			metaData    = null;
			columCount  = 0;
			totalFetchedNum = 0;
			isInited.set(false);
		}
	}
}
