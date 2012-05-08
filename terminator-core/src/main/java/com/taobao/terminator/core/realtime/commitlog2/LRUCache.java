package com.taobao.terminator.core.realtime.commitlog2;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 简单的LRU缓存，支持Closeable的对象在remove的时候执行close()方法
 * @author yusen
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

	private static final long serialVersionUID = -2543651892123179168L;

	private int limit = 100;

	public LRUCache(int limit) {
		super(10, 0.75F, true);
		this.limit = limit;
	}

	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		if(size() > limit) {
			final V v = eldest.getValue();
			if(v instanceof Closeable) {
				try {
					((Closeable) v).close();
				} catch (IOException e) {
					//Nothing to do
				} 
			}
			return true;
		}
		return false;
	}
}
