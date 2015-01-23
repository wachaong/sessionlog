package com.autohome.adrd.algo.sessionlog.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.autohome.adrd.algo.sessionlog.util.Util;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class InputPathFilter implements PathFilter {

	public boolean accept(Path path) {
		String pathStr = path.getName();
		if (Util.isNotBlank(pathStr)) {
			if (pathStr.endsWith("tmp") || pathStr.contains("_SUCCESS") || pathStr.contains("_logs"))
				return false;
		} else
			return false;
		return true;
	}

}
