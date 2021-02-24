package com.shangbaishuyao.hadoop.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Desc: 取第一个KV即可,也就是价格最高的 <br/>
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 11:42 2021/2/24
 */
public class OrderReducer extends  Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
	
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		// 取第一个KV即可， 也就是价格最高的
		context.write(key, NullWritable.get());
	}
}	
