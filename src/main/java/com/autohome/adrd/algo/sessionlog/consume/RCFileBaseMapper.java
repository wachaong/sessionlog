package com.autohome.adrd.algo.sessionlog.consume;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.autohome.adrd.algo.protobuf.AdLogOldOperation;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.sessionlog.config.ConfigLoader;
import com.autohome.adrd.algo.sessionlog.config.PathConfig;
import com.autohome.adrd.algo.sessionlog.config.SessionLogConfig;
import com.autohome.adrd.algo.sessionlog.util.Util;
/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class RCFileBaseMapper<KEYOUT, VALUEOUT> extends
		Mapper<LongWritable, BytesRefArrayWritable, KEYOUT, VALUEOUT> {
	protected String projection = null;
	protected NamedList<Object> list;
	protected SessionLogConfig config = null;
	protected HashMap<String, Integer> idx = new HashMap<String, Integer>();
	private static final Log LOG = LogFactory.getLog(RCFileBaseMapper.class);

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		config = ConfigLoader.loadConfig(conf);
		projection = conf.get("mapreduce.lib.table.input.projection");
		list = new NamedList<Object>();
		idx.put("user", 0);
		int i;
		for (i = 1; i <= config.getPathConfigs().size(); i++) {
			PathConfig pathConfig = config.getPathConfigs().get(i-1);
			System.out.println("pathConfig.getOp(): "+pathConfig.getOp()+" i:"+i);
			idx.put(pathConfig.getOp(), i);
		}
		idx.put("filter", i);
	}

	protected void map(LongWritable key, BytesRefArrayWritable value, Context context)
			throws IOException, InterruptedException {

	}

	protected void decode(LongWritable key, BytesRefArrayWritable value) throws IOException {

		if (Util.isBlank(projection) || value == null || list == null) {
			throw new IllegalArgumentException("illegal projection/protocol/tuple/list");
		}
		if (list.size() > 0)
			list.clear();
		String cgs[] = projection.split(",");
		BytesRefWritable data = null;
		int length = 0;
		
		for (String cg : cgs) {
			data = value.get(idx.get(cg));
			if (data == null || (length = data.getLength()) == 0) continue;
			ByteArrayInputStream input = new ByteArrayInputStream(data.getBytesCopy());
			if (cg.equalsIgnoreCase("user")) {
				String userid = new String(data.getBytesCopy());
				userid = userid == null ? "" : userid;
				list.add("user", userid);
			} else if (cg.equalsIgnoreCase("adoldpv")) {
				AdLogOldOperation.AdPVOldInfoList adpv_lst = AdLogOldOperation.AdPVOldInfoList.parseFrom(input);
				list.add(cg, adpv_lst.getPvList());
			} else if (cg.equalsIgnoreCase("adoldclk")) {
				AdLogOldOperation.AdCLKOldInfoList adpv_lst = AdLogOldOperation.AdCLKOldInfoList.parseFrom(input);
				list.add(cg, adpv_lst.getClkList());
			} else if (cg.equalsIgnoreCase("adpv")) {
				AdLogOperation.AdPVInfoList adpv_lst = AdLogOperation.AdPVInfoList.parseFrom(input);
				list.add(cg, adpv_lst.getPvList());
			} else if (cg.equalsIgnoreCase("adclk")) {
				AdLogOperation.AdCLKInfoList adpv_lst = AdLogOperation.AdCLKInfoList.parseFrom(input);
				list.add(cg, adpv_lst.getClkList());
			} else if (cg.equalsIgnoreCase("pv")) {
				PvlogOperation.AutoPVInfoList pv_lst = PvlogOperation.AutoPVInfoList.parseFrom(input);
				list.add(cg, pv_lst.getPvlogList());
			} else if (cg.equalsIgnoreCase("filter")) {
				String filter = new String(data.getBytesCopy());
				list.add(cg, filter);
			} else {
				throw new UnsupportedOperationException("now not supported cg:" + cg);
			}
		}
		
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		list.clear();
	}

}
