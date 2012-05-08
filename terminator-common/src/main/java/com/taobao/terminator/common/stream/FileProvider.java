package com.taobao.terminator.common.stream;

import java.io.File;

/**
 * 文件的提供方需要实现此接口并将其注入到FileGetServer中 
 */
public interface FileProvider {
	File getTargetFile(String name);
}
