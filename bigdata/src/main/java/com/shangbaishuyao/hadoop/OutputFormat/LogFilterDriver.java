package com.shangbaishuyao.hadoop.OutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Desc:
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 16:50 2021/2/24
 */
public class LogFilterDriver {
	
	public static void main(String[] args) throws Exception {
		args = new String[] { "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/OutputFormat/data", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/OutputFormat/out" };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(LogFilterDriver.class);
		job.setMapperClass(LogFilterMapper.class);
		job.setReducerClass(LogFilterReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 要将自定义的输出格式组件设置到job中
		job.setOutputFormatClass(LogFilterOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
		// 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
