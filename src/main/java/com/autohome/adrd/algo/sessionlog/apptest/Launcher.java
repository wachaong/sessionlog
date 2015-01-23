package com.autohome.adrd.algo.sessionlog.apptest;


import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class Launcher {
	public static void main(String[] args) throws Exception {
			
		if (args.length == 0) {
			System.err.println("Usage: Launcher Processor args...");
			System.exit(-1);
		}
		Object processor = Class.forName(args[0]).newInstance();
		if (processor instanceof AbstractProcessor) {
			String[] paras = new String[args.length - 1];
			System.arraycopy(args, 1, paras, 0, paras.length);
			int ret = ToolRunner.run((AbstractProcessor) processor, paras);
			if (ret != 0) {
				System.err.println("Job Failed!");
				System.exit(ret);
			}
		}
		else {
			System.err.println("Given Processor should be an instance of AbstractProcessor or AbstractProcessorMulti");
			System.exit(-1);
		}
	}
}
