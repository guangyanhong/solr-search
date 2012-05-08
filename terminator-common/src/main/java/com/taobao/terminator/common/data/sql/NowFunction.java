package com.taobao.terminator.common.data.sql;

import java.util.Date;

public class NowFunction implements SqlFunction {

	@Override
	public String getPlaceHolderName() {
		return "now";
	}

	@Override
	public String getValue() {
		return SqlUtils.parseDate(new Date());
	}

}
