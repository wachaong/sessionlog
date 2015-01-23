package com.autohome.adrd.algo.sessionlog.sessionlog_verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.protobuf.NewAdLogOperation;
import com.autohome.adrd.algo.sessionlog.apptest.AbstractProcessor;
/**
 * 
 * 
 * @author [Wangchao: chaowangbj8764@sohu-inc.com ]
 */

public class AdVerify extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public static final String CG_USER = "user";
		public static final String CG_CLK_NEW = "adclick_new";
		public static final String CG_ADDISPLAY_NEW = "addisplay_new";
		

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,adclick_new,addisplay_new");
		}
		
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {

			List<NewAdLogOperation.AdCLKInfo> clkList = new ArrayList<NewAdLogOperation.AdCLKInfo>();
			List<NewAdLogOperation.AdPVInfo> pvList = new ArrayList<NewAdLogOperation.AdPVInfo>();
			decode(key, value);
	
			clkList = (List<NewAdLogOperation.AdCLKInfo>) list.get(CG_CLK_NEW);
			pvList = (List<NewAdLogOperation.AdPVInfo>) list.get(CG_ADDISPLAY_NEW);
									
			if (clkList != null && clkList.size() != 0) {				
				context.write(new Text("clk"), new Text(String.valueOf(clkList.size())));
				context.write(new Text("clk_uv"), new Text("1"));
			}
			
			if (pvList != null && pvList.size() != 0) {				
				context.write(new Text("pv"), new Text(String.valueOf(pvList.size())));
				context.write(new Text("pv_uv"), new Text("1"));
			}
		}
	}
	
	public static class HCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			long cnt=0;
			for (Text value : values) {
				cnt+=Long.valueOf(value.toString());				
			}
			context.write(key, new Text(String.valueOf(cnt)));
		}
	}	
	
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long cnt = 0;
			for (Text value : values) {
				cnt+=Long.valueOf(value.toString());				
			}						
			context.write(key, new Text(String.valueOf(cnt)));
		}
	}
		
	
	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setReducerClass(HReduce.class);
		job.setCombinerClass(HCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}	
}