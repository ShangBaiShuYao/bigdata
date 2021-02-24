package com.shangbaishuyao.hadoop.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Desc: 需求: 统计过程中对每一个MapTask的输出进行局部汇总，以减小网络传输量即采用Combiner功能。 <br/>
 * create by shangbaishuyao on 2021/2/23
 * @Author: 上白书妖
 * @Date: 11:40 2021/2/23
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0 ; 
		
		for (IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		v.set(sum);
		context.write(key, v);
	
	}	
}
