package com.shangbaishuyao.hadoop.GroupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Desc: 驱动类<br/>
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 11:42 2021/2/24
 */

public class OrderDriver {

	public static void main(String[] args) throws Exception {
		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/GroupingComparator/data", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/GroupingComparator/out" };

		// 1 获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 设置jar包加载路径
		job.setJarByClass(OrderDriver.class);

		// 3 加载map/reduce类
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// 4 设置map输出数据key和value类型
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 设置最终输出数据的key和value类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 设置输入数据和输出数据路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 设置reduce端的分组
		job.setGroupingComparatorClass(OrderWritableComparator.class);

		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
