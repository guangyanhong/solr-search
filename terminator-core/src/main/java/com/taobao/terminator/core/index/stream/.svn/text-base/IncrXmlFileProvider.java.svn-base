package com.taobao.terminator.core.index.stream;

import java.io.File;
import java.text.ParseException;

import com.taobao.terminator.common.constant.IndexEnum;
import com.taobao.terminator.common.stream.FileProvider;
import com.taobao.terminator.core.util.IndexFileUtils;

/**
 * 增量xml文件提供者
 * 
 * @author yusen
 *
 */
public class IncrXmlFileProvider implements FileProvider{
	public static final String type = "increIndexFile";
	private File baseDir = null;
	
	public IncrXmlFileProvider(File baseDir) {
		this.baseDir = baseDir;
	}

	@Override
	public File getTargetFile(String name) {
		String path = null;
		try {
			path = IndexFileUtils.generatePathFromFileName(baseDir, name, IndexEnum.INDEX_FILE_SUFFIX.getValue());
		} catch (ParseException e) {
			return null;
		}
		return new File(path);
	}
}
