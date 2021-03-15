package com.shangbaishuyao.hadoop.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
/**
 * Desc: 驱动类 <br/>
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 22:32 2021/2/24
 */
public class MapJoinDriver {
	public static void main(String[] args) throws Exception {
		// 0 根据自己电脑路径重新配置
		args = new String[]{"H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/mapJoin/data", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/mapJoin/out"};

		// 1 获取job信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置加载jar包路径
		job.setJarByClass(MapJoinDriver.class);

		// 3 关联map
		job.setMapperClass(MapJoinMapper.class);
		
		// 4 设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 5 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6 加载缓存数据. 将小表内容加载到缓存中.
		job.addCacheFile(new URI("file:///H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/mapJoin/data2/pd.txt"));
		
		// 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
		job.setNumReduceTasks(0);

		// 8 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}

