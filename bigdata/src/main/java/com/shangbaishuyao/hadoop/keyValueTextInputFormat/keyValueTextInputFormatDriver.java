package com.shangbaishuyao.hadoop.keyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class keyValueTextInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/keyValueTextInputFormat","H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/keyValueTextInputFormat/out" };

        Configuration conf = new Configuration();
        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 1 获取job对象
        Job job = Job.getInstance(conf);

        // 2 设置jar包位置，关联mapper和reducer
        job.setJarByClass(keyValueTextInputFormatDriver.class);
        job.setMapperClass(keyValueTextInputFormatMapper.class);
        job.setReducerClass(keyValueTextInputFormatReducer.class);

        // 3 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 6 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        job.waitForCompletion(true);
    }
}
