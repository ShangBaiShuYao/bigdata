package com.shangbaishuyao.hadoop.Partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Desc: Driver驱动类 <br/>
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 16:17 2021/2/20
 */
public class FlowsumDriver {
    public static void main(String[] args) throws Exception {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/Partition", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/Partition/out" };

        //1. 创建Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2. 关联Jar
        job.setJarByClass(FlowsumDriver.class);

        //3. 关联Mapper 和 Reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4. 设置Mapper输出的key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5. 设置最终输出的key  和  value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 设置分区器
        job.setPartitionerClass(phoneNumberPartitioner.class);
        // 设置ReduceTask的个数
        job.setNumReduceTasks(5);

        //7. 提交Job
        job.waitForCompletion(true);
    }
}
