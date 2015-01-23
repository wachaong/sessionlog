package com.autohome.adrd.algo.sessionlog.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.autohome.adrd.algo.sessionlog.config.ConfigLoader;
import com.autohome.adrd.algo.sessionlog.config.PathConfig;
import com.autohome.adrd.algo.sessionlog.config.SessionLogConfig;
import com.autohome.adrd.algo.sessionlog.interfaces.AbstractProtobuf;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.Util;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class SessionLogMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
//public class SessionLogMapper extends Mapper<LongWritable, Text, Text, Text> {

	private SessionLogConfig config = null;
	private AbstractProtobuf extractor = null;
	private byte idx;

	private ByteArrayOutputStream buffer = null;
	private DataOutputStream output = null;
	private static final Log LOG = LogFactory.getLog(SessionLogMapper.class);

	public static String BytesToStr(byte[] target)
	 {
	  StringBuffer buf = new StringBuffer();
	  for (int i = 0, j = target.length; i < j; i++) {
	   buf.append(String.valueOf((char) target[i]-0)+" ");
	  }
	  return buf.toString();
	 }
	
	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		config = ConfigLoader.loadConfig(conf);
		if (config == null || !config.check())
			throw new IOException("sessionlog.config logic error");
		String splitPath = ((FileSplit) context.getInputSplit()).getPath().toString();
		int cnt = 1;
		for (PathConfig pathConfig : config.getPathConfigs()) {
			if (splitPath.startsWith(pathConfig.getLocation())) {				
				idx = (byte)cnt;
				String extract = "com.autohome.adrd.algo.sessionlog.plugin." + pathConfig.getOp();
				@SuppressWarnings("rawtypes")
				Class extractorClazz = null;
				try {
					extractorClazz = Class.forName(extract);
				} catch (ClassNotFoundException e) {
					throw new IOException("Class for formator/extractor" + " not found, " + "check distributedCache clas path configed");
				}
				extractor = (AbstractProtobuf) ReflectionUtils.newInstance(extractorClazz, null);				
				break;
			}
			cnt ++;
		}
		if (extractor == null) {
			throw new RuntimeException(splitPath + " format or extractor not configed in sessionlog.config");
		}

	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String strValue = value.toString();
		if (Util.isBlank(strValue)) {
			return;
		}

		String userKey = null;						
		// Extract
		List<ExtractorEntry> entryList = extractor.extract(strValue);
		// Write key and value
		for (ExtractorEntry entry : entryList) {
			if (entry == null || !entry.check()) {
				continue;
			}
			if (output == null) {
				buffer = new ByteArrayOutputStream();
				output = new DataOutputStream(buffer);
			}
			if (userKey == null)
				userKey = entry.getUserKey();
			if (userKey == null)
				userKey = "";
			output.write(idx);  // data_source  :  column  now 1:1, used to be m:n
			output.writeLong(entry.getTimestamp());
			output.write(entry.getData());				
			context.write(new Text(userKey), new BytesWritable(buffer.toByteArray()));
			buffer.reset();
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		try {
			if (output != null)
				output.close();
		} catch (IOException e) {
		}
	}

}