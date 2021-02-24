package com.shangbaishuyao.hadoop.OutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 16:50 2021/2/24
 */
public class LogFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		context.write(key, NullWritable.get());
	}
}
