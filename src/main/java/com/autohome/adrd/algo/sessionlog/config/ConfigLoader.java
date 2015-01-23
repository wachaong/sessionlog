package com.autohome.adrd.algo.sessionlog.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.digester3.Digester;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.xml.sax.SAXException;

import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class ConfigLoader {

	private static final Log LOG = LogFactory.getLog(ConfigLoader.class);
	public static String SESSIONLOG_CONFIG_NAME = "sessionlog.config";

	public static SessionLogConfig loadConfig(Configuration conf) throws IOException {

		SessionLogConfig config = null;
		URI localFiles[] = DistributedCache.getCacheFiles(conf);
		System.out.println("local file:"+localFiles.toString());
		if (localFiles == null || localFiles.length == 0)
			throw new IOException("sessionlog.config excepted");
		IOException exception = null;
		for (URI uri : localFiles) {
			LOG.warn("config path:" + uri.toString());

			if (uri != null && uri.getFragment().contains(SESSIONLOG_CONFIG_NAME)) {
				Path path = new Path(uri.getRawPath());
				System.out.println("rawpath:"+uri.getRawPath());
				FileSystem fs = path.getFileSystem(conf);
				if (fs.exists(path)) {
					try {
						InputStream input = fs.open(path);
						Digester digester = new Digester();
						config = new SessionLogConfig();
						digester.push(config);
						digester.addObjectCreate("config/paths/path", PathConfig.class);
						digester.addSetProperties("config/paths/path");
						digester.addSetNext("config/paths/path", "addPathConfig");
												
						config = (SessionLogConfig) digester.parse(input);
						break;
					} catch (Exception e) {
						exception = new IOException("sessionlog.config parse error");
						break;
					}
				} else {
					exception = new IOException("sessionlog.config excepted");
					break;
				}
			}
		}
		if (exception != null)
			throw exception;
		return config;
	}
	

	
	public static void main(String args[])  throws IOException, SAXException
	{		
		File file = new File("D:/gittest/sessionlog.config");
		InputStream inputStream = new FileInputStream(file);
		
		HashMap<String, AbstractProtobuf> processors = new HashMap<String, AbstractProtobuf>();
		Digester digester = new Digester();
		SessionLogConfig config = new SessionLogConfig();
		digester.push(config);
		digester.addObjectCreate("config/paths/path", PathConfig.class);
		digester.addSetProperties("config/paths/path");
		digester.addSetNext("config/paths/path", "addPathConfig");
		config = (SessionLogConfig) digester.parse(inputStream);
		System.out.println("config:"+config.toString());
		/*
		config = (SessionLogConfig) digester.parse(inputStream);

		for (int i = 0; i < config.getOpConfigs().size(); i++) {
			OpConfig opConfig = config.getOpConfigs().get(i);
			try {
				Class<Processor> processClass = (Class<Processor>)Class.forName(opConfig.getProcessor());
				System.out.println(processClass);
				
				processors.put(opConfig.getOperation(), (Processor)ReflectionUtils.newInstance(processClass, null));
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Class for processor" + " not found, " +
				"check distributedCache class path configed");
			}
		}
		*/
	}

}
