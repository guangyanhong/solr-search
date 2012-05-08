package com.taobao.terminator.common.data;

import java.util.List;
import java.util.Map;

import com.taobao.terminator.common.data.SubtableDescParseException;

public interface TableDescriptionParser {
	public Map<String, List<String>> parse(String raw) throws SubtableDescParseException;
}
