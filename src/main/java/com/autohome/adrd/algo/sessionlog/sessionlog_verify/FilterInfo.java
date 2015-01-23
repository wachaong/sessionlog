package com.autohome.adrd.algo.sessionlog.sessionlog_verify;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.autohome.adrd.algo.sessionlog.apptest.AbstractProcessor;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;

/**
 * 
 * 
 * @author [huawei: huawei@autohome.com.cn ]
 */

public class FilterInfo extends AbstractProcessor{
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public static final String CG_USER = "user";
		public static final String CG_FILTER = "filter";

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,filter");
		}
		
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {

			decode(key, value);	
			String cookie = (String)list.get(CG_USER);						
			String info = (String)list.get(CG_FILTER);
			
			if(info != null)			
				context.write(new Text(cookie), new Text(info));
			
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

}
