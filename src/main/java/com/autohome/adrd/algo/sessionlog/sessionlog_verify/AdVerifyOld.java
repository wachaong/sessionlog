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
import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
import com.autohome.adrd.algo.sessionlog.apptest.AbstractProcessor;
/**
 * 
 * 
 * @author [Wangchao: chaowangbj8764@sohu-inc.com ]
 */

public class AdVerifyOld extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public static final String CG_USER = "user";
		public static final String CG_ADDISPLAY_NEW = "adoldpv";
		

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,adoldpv,filter");
		}
		
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {

			List<AdLogOldOperation.AdPVOldInfo> pvList = new ArrayList<AdLogOldOperation.AdPVOldInfo>();
			decode(key, value);
	
			pvList = (List<AdLogOldOperation.AdPVOldInfo>) list.get(CG_ADDISPLAY_NEW);
			String cookie = (String) list.get("user");
			
			StringBuilder sb = new StringBuilder();

			if (pvList != null && pvList.size() != 0) {
				for(AdLogOldOperation.AdPVOldInfo pvinfo : pvList)
				{
					//context.write(new Text(pvinfo.getCreativeid()), new Text("1"));
					sb.append(pvinfo.getCarid() + ",");
					sb.append(pvinfo.getIp() + ",");
					sb.append(pvinfo.getPvid() + ",");
					sb.append(pvinfo.getCookie() + ",");	
				}				
			}
			context.write(new Text(cookie), new Text(sb.toString()));
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