package com.shangbaishuyao.hadoop.GroupingComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc:  NullWritable 表示没有类型
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 11:42 2021/2/24
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	
	OrderBean  k = new OrderBean();
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 获取一行数据 
		 //  0000001	Pdt_01	222.8
		String line = value.toString() ;
		//切分
		String [] splits = line.split("\t");
		
		k.setOrder_id(Integer.parseInt(splits[0]));
		k.setPrice(Double.parseDouble(splits[2]));
		
		//写出
		context.write(k, NullWritable.get());
	}
}
