package com.shangbaishuyao.hadoop.OutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 16:50 2021/2/24
 */
public class LogFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String log = value.toString();
		
		log = log + "\r\n";
		
		v.set(log);

		context.write(v, NullWritable.get());
	}
}	
