package com.shangbaishuyao.hadoop.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Desc: 驱动类<br/>
 *
 * create by shangbaishuyao on 2021/2/23
 * @Author: 上白书妖
 * @Date: 11:40 2021/2/23
 */
public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
		
		// FileAlreadyExistsException: Output directory file:/D:/output already exists
		args = new String [] {"H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/Combiner" , "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/Combiner/out"};
		
		//1. 创建一个Job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2. 关联jar
		job.setJarByClass(WordCountDriver.class);
		//3. 关联Mapper 和 Reuder
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//4 设置Mapper输出的key 和 value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5. 设置最终输出的key  和  Value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6. 设置输入和 输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// 演示CombineTextInputFormat
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);
		
		//job.setNumReduceTasks(2);
		
		
		// 设置使用Combiner
		//job.setCombinerClass(WordCountCombiner.class);
		job.setCombinerClass(WordCountReducer.class);  //因为我的Combiner是继承了Reducer,我的WordCountReducer也是继承Reducer,所以他们是一样的东西,所以也可以这么写
		
		//7. 提交Job
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
		
	}
}
