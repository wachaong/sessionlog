package com.autohome.adrd.algo.sessionlog.apptest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation.TargetingInfo.Brand;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class LabelInstance extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public static final String CG_USER = "user";
		public static final String CG_ADDISPLAY = "addisplay";
		public static final String CG_CLK = "adclick";
		public static final String CG_PV = "pv";
		public static final String CG_FILTER = "filter";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
			
		private static String in_encoding;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);		
			in_encoding = context.getConfiguration().get("in_encoding", "utf-8");
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick,pv");
		}
		
		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			//List<AdLogOperation.AdPVInfo> pvList = new ArrayList<AdLogOperation.AdPVInfo>();
			//List<AdLogOperation.AdCLKInfo> clkList = new ArrayList<AdLogOperation.AdCLKInfo>();
			//List<PvlogOperation.AutoPVInfo> pvlogList = new ArrayList<PvlogOperation.AutoPVInfo>();
			
			List<ApplogOperation.AutoAppInfo> apppvlogList = new ArrayList<ApplogOperation.AutoAppInfo>();
			
			//TargetingKVOperation.TargetingInfo taginfo  = null;

			decode(key, value);
	
			apppvlogList = (List<ApplogOperation.AutoAppInfo>) list.get(CG_APPPV);
			//pvList = (List<AdLogOperation.AdPVInfo>) list.get(CG_ADDISPLAY);
			//clkList = (List<AdLogOperation.AdCLKInfo>) list.get(CG_CLK);
			//pvlogList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			String cookie = (String)list.get(CG_USER);
			String info = (String)list.get(CG_FILTER);
			//taginfo = (TargetingKVOperation.TargetingInfo)list.get(CG_TAGS);
			
			//pvList = (List<AdLogOperation.AdPVInfo>) list.get(CG_ADDISPLAY);
			/*
			if(taginfo != null )
			{
				StringBuilder sb = new StringBuilder();
				sb.append(taginfo.getCookie());
				sb.append(taginfo.getCookieTime());
				for( Brand brand : taginfo.getBrandListList())
				{
					sb.append(brand.getBrandid());
					sb.append(",");
					sb.append(brand.getScore());
				}
				
				context.write(new Text(cookie), new Text(sb.toString()));
			}
			*/
			
			StringBuilder sb = new StringBuilder();
			
			if (apppvlogList != null && apppvlogList.size() != 0) {
				for(ApplogOperation.AutoAppInfo pvinfo : apppvlogList)
				{
					sb.append(pvinfo.getDeviceid());
					sb.append("\t");
					//sb.append(pvinfo.getCurrenturl());
					sb.append("\t");
				}
				context.write(new Text(cookie), new Text(sb.toString()));
			}
			
			
			/*
			if (clkList != null && clkList.size() != 0) {
				for(AdLogOperation.AdCLKInfo clkinfo : clkList)
				{
					sb.append(clkinfo.getCreativeid());
					sb.append("\t");
				}
				sb.append(":pvlog:");
			}

			if (pvlogList != null && pvlogList.size() != 0) {
				for(PvlogOperation.AutoPVInfo pvloginfo : pvlogList)
				{
					sb.append(pvloginfo.getIp());
					sb.append("\t");
				}
			}
						*/
			
			//context.write(new Text(cookie), new Text(sb.toString()));
			
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