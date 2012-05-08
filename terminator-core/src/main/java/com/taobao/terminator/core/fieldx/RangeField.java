package com.taobao.terminator.core.fieldx;


/**
 * 区间查询的标识字段类型
 * 
 * @author yusen
 *
 */
public interface RangeField{
	public static final int TYPE_SHORT = 1;
	public static final int TYPE_INT = 2;
	public static final int TYPE_LONG = 3;
	public int getType();
	
	public static class Utils {
		public static String genRFName(String fn,int type) {
			if (type == RangeField.TYPE_SHORT) {
				return "RF_S_" + fn;
			} else if (type == RangeField.TYPE_INT) {
				return "RF_I_" + fn;
			} else if (type == RangeField.TYPE_LONG) {
				return "RF_L_" + fn;
			} else {
				throw new IllegalArgumentException("ERROR type {" + type + "}");
			}
		}
	}
}
