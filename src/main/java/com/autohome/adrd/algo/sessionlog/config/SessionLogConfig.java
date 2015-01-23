package com.autohome.adrd.algo.sessionlog.config;

import java.util.ArrayList;

import com.autohome.adrd.algo.sessionlog.util.Util;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class SessionLogConfig {
	
	private ArrayList<PathConfig> pathConfigs = new ArrayList<PathConfig>();
	
	public void addPathConfig(PathConfig pathConfig) {
		pathConfigs.add(pathConfig);
	}
	
	public ArrayList<PathConfig> getPathConfigs() {
		return pathConfigs;
	}
	
	public void setPathConfigs(ArrayList<PathConfig> pathConfigs) {
		this.pathConfigs = pathConfigs;
	}
		
	public boolean check() {
		for (PathConfig pathConfig : pathConfigs) {
			if (pathConfig == null || Util.isBlank(pathConfig.getLocation()) ||
			Util.isBlank(pathConfig.getOp())) {
				return false;
			}
		}
		return true;
	}
		
}
