package com.taobao.terminator.client.index.data;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 此DataProvider比较特殊，在Init阶段一次性把所有的数据load到内存，然后才开始迭代内存中的数据<br>
 * 这样做的目的是降低数据库连接的时间，应用于dbconnector比较少，且load的数据不多的场景，如果load的数据较多的话即有可能会OOM<br>
 * <br>
 * 
 * 注意！！！！OOM，一般情况下不推荐使用这个DataProvider
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
			throw new DataProviderException("一次性获取所有数据失败", e);
		} finally {
			logger.warn("释放数据库连接资源..");
			// 获取数据完毕之后立马释放数据库连接资源
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
		logger.warn("一次性取出所有的数据。。");
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
		logger.warn("一次性取出数据的条数 ==> {" + rows.size() +"}");
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
