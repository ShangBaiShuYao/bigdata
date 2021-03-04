package com.shangbaishuyao.hive;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GuLiVideoMapper  extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//获取一行
		String line  = value.toString();
		
		//清洗
		String result = ETLUtils.etlData(line);
		
		if(result == null ) {
			return ; 
		}
		
		//写出
		k.set(result);
		
		context.write(k, NullWritable.get());
	
	}
}
