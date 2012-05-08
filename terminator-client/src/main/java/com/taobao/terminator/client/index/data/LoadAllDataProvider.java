package com.taobao.terminator.client.index.data;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ��DataProvider�Ƚ����⣬��Init�׶�һ���԰����е�����load���ڴ棬Ȼ��ſ�ʼ�����ڴ��е�����<br>
 * ��������Ŀ���ǽ������ݿ����ӵ�ʱ�䣬Ӧ����dbconnector�Ƚ��٣���load�����ݲ���ĳ��������load�����ݽ϶�Ļ����п��ܻ�OOM<br>
 * <br>
 * 
 * ע�⣡������OOM��һ������²��Ƽ�ʹ�����DataProvider
 * 
 * 
 * @author yusen
 * 
 */
public class LoadAllDataProvider extends PrepareSingleTableDataProvider {
	private Iterator<Map<String, String>> iter;
	private List<Map<String, String>> rows;

	@Override
	public void init() throws DataProviderException {
		try {
			super.init();
			this.fetchAll();
		} catch (Exception e) {
			throw new DataProviderException("һ���Ի�ȡ��������ʧ��", e);
		} finally {
			logger.warn("�ͷ����ݿ�������Դ..");
			// ��ȡ�������֮�������ͷ����ݿ�������Դ
			this.closeJDBC();
		}
	}

	private void closeJDBC() throws DataProviderException {
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				prestmt.close();
			if (connnection != null)
				connnection.close();
		} catch (SQLException e) {
			throw new DataProviderException("single-close-error", e);
		} finally {
			resultSet = null;
			prestmt = null;
			connnection = null;
			metaData = null;
			columCount = 0;
			totalFetchedNum = 0;
			isInited.set(false);
		}
	}

	private void fetchAll() throws SQLException {
		logger.warn("һ����ȡ�����е����ݡ���");
		if (rows == null) {
			rows = new ArrayList<Map<String, String>>();
		} else {
			rows.clear();
		}

		while (resultSet.next()) {
			Map<String, String> row = new HashMap<String, String>(columCount);
			for (int i = 1; i <= columCount; i++) {
				String key = metaData.getColumnLabel(i);
				String value = resultSet.getString(i);
				row.put(key, value != null ? value : " ");
			}
			rows.add(row);
		}
		logger.warn("һ����ȡ�����ݵ����� ==> {" + rows.size() +"}");
		iter = rows.iterator();
	}

	@Override
	public boolean hasNext() throws DataProviderException {
		return iter.hasNext();
	}

	@Override
	public Map<String, String> next() throws DataProviderException {
		return iter.next();
	}

	@Override
	public void close() throws DataProviderException {
		try {
			if (rows != null) {
				rows.clear();
			}
		} finally {
			rows = null;
		}
	}
}
