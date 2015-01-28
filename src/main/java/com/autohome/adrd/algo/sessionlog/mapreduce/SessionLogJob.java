package com.autohome.adrd.algo.sessionlog.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.autohome.adrd.algo.sessionlog.config.ConfigLoader;
import com.autohome.adrd.algo.sessionlog.format.ConfigurableTextInputFormat;
import com.autohome.adrd.algo.sessionlog.format.InputPathFilter;
import com.autohome.adrd.algo.sessionlog.util.Util;
import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class SessionLogJob implements Tool {

	private CommandLineParser parser = new BasicParser();
	private Options allOptions;
	private Configuration config_;
	private String[] argv_;
	private String output_;
	private String input_;
	private String time_;
	private String configFile_;
	private String numReduceTasks_;
	private String jobName_;
	private String attachments_;

	private int numReduce = 1;
	private FileSystem client = null;
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
	private static final Log LOG = LogFactory.getLog(SessionLogJob.class);

	public static void main(String[] args) throws Exception {

		int returnStatus = 0;
		SessionLogJob job = new SessionLogJob();
		
		returnStatus = ToolRunner.run(job, args);
		if (returnStatus != 0) {
			System.err.println("Sessionlog Job Failed!");
			System.exit(returnStatus);
		}
	}

	public SessionLogJob() {
		setupOptions();
		this.config_ = new Configuration();
	}

	private void setupOptions() {
		Option time = createOption("time", "DFS input time for the Map step", "yyyy/MM/dd", Integer.MAX_VALUE, false);
		Option input = createOption("input", "DFS input file(s) for the Map step", "path", Integer.MAX_VALUE, true);
		Option output = createOption("output", "DFS output directory for the Reduce step", "path", 1, true);
		Option configFile = createOption("configFile", "sessionlog config file path", "file", Integer.MAX_VALUE, true);
		Option numReduceTasks = createOption("numReduceTasks", "reducer num", "spec", 1, true);
		Option attachments = createOption("attachments", "sessionlog attachments, comma seperated", "file", Integer.MAX_VALUE, false);
		Option jobName = createOption("jobName", "Optional.", "spec", 1, false);

		allOptions = new Options().addOption(time).addOption(input).addOption(output).addOption(configFile).addOption(numReduceTasks).addOption(attachments).addOption(jobName);
	}

	public void exitUsage() {
		System.out.println("Usage: $HADOOP_HOME/bin/hadoop jar $SESSIIONLOG_HOME/sessionlog-$VERSION.jar [options]");
		System.out.println("Options:");
		System.out.println("  -time <yyyy/MM/dd> DFS input time for the Map step");
		System.out.println("  -input <path> DFS input file(s) for the Map step");
		System.out.println("  -output <path> DFS output directory for the Reduce step");
		System.out.println("  -configFile <path> session log job config file");
		System.out.println("  -numReduceTasks <int> session log job reduce number");
		System.out.println("  -jobName <string> session log MR job name");
		System.out.println("  -attachments <files> session log attachments (mkeylist file e.g.)");
	}

	public void fail(String message) {
		System.err.println(message);
		throw new IllegalArgumentException(message);
	}

	@SuppressWarnings("static-access")
	private Option createOption(String name, String desc, String argName, int max, boolean required) {
		return OptionBuilder.withArgName(argName).hasArgs(max).withDescription(desc).isRequired(required).create(name);

	}

	public Configuration getConf() {
		return config_;
	}

	public void setConf(Configuration conf) {
		this.config_ = conf;
	}

	public int run(String[] args) throws Exception {
		try {
			StringBuffer sb = new StringBuffer();
			for (String arg : args)
				sb.append(arg).append(" ");
			LOG.warn("submit sessionlog job : " + sb.toString());
			this.argv_ = args;
			parseArgv();
			postProcessArgs();
			setJobConf();
			return submitAndMonitorJob();
		} catch (IllegalArgumentException ex) {
			LOG.warn("submit sessionlog job error for : " + ex);
			return 1;
		}
	}

	private void parseArgv() {
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(allOptions, argv_);
		} catch (Exception oe) {
			exitUsage();
			fail("parser cmd error:" + oe.toString());
		}
		if (cmdLine != null) {
			time_ = (String) cmdLine.getOptionValue("time");
			input_ = (String) cmdLine.getOptionValue("input");
			output_ = (String) cmdLine.getOptionValue("output");
			configFile_ = (String) cmdLine.getOptionValue("configFile");
			numReduceTasks_ = (String) cmdLine.getOptionValue("numReduceTasks");
			jobName_ = (String) cmdLine.getOptionValue("jobName");
			attachments_ = (String) cmdLine.getOptionValue("attachments");
		} else {
			exitUsage();
			fail("parser cmd error, is null");
		}
	}

	private void postProcessArgs() throws IOException, ParseException {
		String message = null;
		String timeStr = null;
		String timeStr_using = null;
		// set time
		timeStr = time_;
		String timeStr_trim = timeStr.replace("/", "");
		
        Date d = new SimpleDateFormat("yyyy/MM/dd").parse(time_);
        Calendar c = Calendar.getInstance();
        c.setTime(d);  
        int day = c.get(Calendar.DATE);  
        c.set(Calendar.DATE, day - 1);  
        String dayBefore = new SimpleDateFormat("yyyy/MM/dd").format(c.getTime());  
		
		// set input path
		if (Util.isBlank(input_))
			message = "input path not configed";
		else {
			String parts[] = input_.split(",");
			if (parts == null || parts.length == 0)
				message = "input path not configed error";
			else {
				if (client == null)
					client = FileSystem.get(config_);
				StringBuffer sb = new StringBuffer();
				boolean flag = false;
				for (String part : parts) {
					if (Util.isBlank(part)) {
						message = "input path not configed error";
						break;
					}
					if (part.endsWith("/"))
						part = part.substring(0, part.length() - 1);
					FileStatus fstatus = null;
					try {
						fstatus = client.getFileStatus(new Path(part));
					} catch (Exception e) {
					}
					if (fstatus == null)
						message = "path:" + part + " not found";

					// yyyy/mm/dd/hh
					else if (fstatus.isDir()) {
						if (part.startsWith("/dw/mds/mds_autodealerorder_dealerorders")) {
							// adapter time
							timeStr_using = "dt=" + timeStr_trim;
						} else if (part.startsWith("/dw/mds/mds_bhv_salelead")) {
							// adapter time
							timeStr_using = timeStr_trim;
						} else if (part.startsWith("/dw/targeting/user_tag_whole")) {
							// using the day before
							timeStr_using = dayBefore;					
						} else {
							timeStr_using = timeStr;
						}

						if (
								part.startsWith("/dw/targeting/user_tag_whole") || 
								part.startsWith("/dw/mds/mds_autodealerorder_dealerorders")) {
							String addtpart = part;
							if(! timeStr_using.equals(""))
								addtpart = part + "/" + timeStr_using;
														
							FileStatus subfstatus = null;
							try {
								subfstatus = client.getFileStatus(new Path(addtpart));
							} catch (Exception e) {
							}
							if (subfstatus == null)
								continue;
							if (!flag) {
								flag = true;
								sb.append(addtpart);
							} else
								sb.append(",").append(addtpart);
						} else {
							String addtpart = part + "/" + timeStr_using;
							try {
								FileStatus subs[] = client.listStatus(new Path(addtpart));
								for (FileStatus sub : subs) {
									String subpart = addtpart + "/" + sub.getPath().getName();
									FileStatus subfstatus = null;
									try {
										subfstatus = client.getFileStatus(new Path(subpart));
									} catch (Exception e) {
									}
									if (subfstatus == null)
										continue;
									if (!flag) {
										flag = true;
										sb.append(subpart);
									} else
										sb.append(",").append(subpart);
								}
							} catch (FileNotFoundException e) {
							}

						}
					} else {
						if (!flag) {
							flag = true;
							sb.append(part);
						} else
							sb.append(",").append(part);
					}
				}
				input_ = sb.toString();
				// message = "input path:" + input_;
			}
		}

		// set output path
		if (Util.isBlank(output_))
			message = "output path not configed";
		else {
			String parts[] = output_.split(",");
			if (parts.length > 1)
				message = "output path can't be mutil";
			if (client == null)
				client = FileSystem.get(config_);
			if (output_.endsWith("/"))
				output_ = output_ + timeStr;
			else
				output_ = output_ + "/" + timeStr;
			FileStatus fstatus = null;
			try {
				fstatus = client.getFileStatus(new Path(output_));
			} catch (Exception e) {
			}
			if (fstatus != null)
				message = "output path:" + output_ + " exists";
		}

		// set config file
		if (Util.isBlank(configFile_))
			message = "config file not configed";
		else {
			String files = "";
			for (String conFile : configFile_.split(",")) {
				File file = new File(conFile);
				if (file.isDirectory() || !file.exists())
					message = "config file must be sessionlog.config";
				else
					files += "file://" + conFile + ",";
			}
			configFile_ = files.substring(0, files.length() - 1);
		}
		if (Util.isNotBlank(numReduceTasks_)) {
			try {
				numReduce = Integer.parseInt(numReduceTasks_);
			} catch (NumberFormatException e) {
				message = "reduce task number not int";
			}
		}
		if (client != null)
			client.close();
		if (message != null) {
			exitUsage();
			fail(message);
		}
	}

	private void setJobConf() throws IOException {
		config_.set("tmpfiles", Util.isNotBlank(attachments_) ? configFile_ + "," + attachments_ : configFile_);
		config_.set("mapred.job.name", Util.isNotBlank(jobName_) ? jobName_ : "sessionlog-rc");
		config_.set("mapred.mapper.new-api", "true");
		config_.set("mapred.reducer.new-api", "true");
		config_.set("mapred.input.dir", input_);
		config_.set("mapred.output.dir", output_);
		config_.set("mapred.child.java.opts", "-Xmx8192m");
		String value = Long.toString(4 * 67108864L);
		config_.set("mapred.min.split.size", value);
		config_.set("table.input.split.minSize", value);
		Path outputPath = new Path(output_);
		FileSystem fs = FileSystem.get(outputPath.toUri(), config_);
		if (fs.exists(outputPath)) {
			exitUsage();
			fail("output path exits, check");
		}
	}

	private int submitAndMonitorJob() throws IOException {
		RCFileOutputFormat.setColumnNumber(config_,config_.size());
		Job job = new Job(config_);
		job.setJarByClass(this.getClass());
		//ArrayList <String> schema=ConfigLoader.schema;
		//RCFileOutputFormat.setColumnNumber(config_, LogSchema.SESSIONLOG_SCHEMA.length);
		//RCFileOutputFormat.setColumnNumber(config_,schema.size());
		
		
		FileInputFormat.setInputPathFilter(job, InputPathFilter.class);
		job.setInputFormatClass(ConfigurableTextInputFormat.class);

		job.setOutputFormatClass(RCFileOutputFormat.class);

		job.setMapperClass(SessionLogMapper.class);
		job.setReducerClass(SessionLogReducer.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(numReduce);
		// job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		// job.setMapOutputValueClass(Text.class);

		//job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesRefArrayWritable.class);
		// job.setOutputValueClass(Text.class);

		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		try {
			job.submit();
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		} catch (Exception e) {
			LOG.warn("submit sessionlog job error for : " + e);
			return 1;
		}
	}

}