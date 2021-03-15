package com.shangbaishuyao.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VideoDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//关联jar
		job.setJarByClass(VideoDriver.class);
		
		//关联Mapper
		job.setMapperClass(VideoMapper.class);
		
		//设置Mapper的输出 key  和 value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//设置最终输出的key 和  value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		//设置reduce的个数为0 
		job.setNumReduceTasks(0);
		
		//设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//提交job
		job.waitForCompletion(true);
	}
}
