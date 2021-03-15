package com.shangbaishuyao.hadoop.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Desc:  驱动类 <br/>
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 21:52 2021/2/24
 */
public class ReduceJoinDriver {
	public static void main(String[] args)  throws Exception{
		Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ReduceJoinDriver.class);

        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/reduceJoin/data"));
        FileOutputFormat.setOutputPath(job, new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/reduceJoin/out"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
	}
}
