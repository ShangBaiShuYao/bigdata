package com.shangbaishuyao.hadoop.smallInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
/**
 * Desc: 驱动类<br/>
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:32 2021/2/22
 */
public class SmallFileInputFormatDriver {
    public static void main(String[] args) throws Exception {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/smallInputFormat/data", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/smallInputFormat/out" };

        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar包存储位置、关联自定义的mapper和reducer
        job.setJarByClass(SmallFileInputFormatDriver.class);
        job.setMapperClass(SmallFileInputFormatMapper.class);
        job.setReducerClass(SmallFileInputFormatReducer.class);

        // 7设置输入的inputFormat
        job.setInputFormatClass(SmallFileInputFormat.class);

        // 8设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 3 设置map输出端的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 4 设置最终输出端的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
