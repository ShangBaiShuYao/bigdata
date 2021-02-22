package com.shangbaishuyao.hadoop.keyValueTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:14 2021/2/22
 */
public class keyValueTextInputFormatMapper extends Mapper<Text, Text,Text, IntWritable> {
    IntWritable intWritable =  new IntWritable();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //读进来之后直接写出去
        context.write(key,intWritable);
    }
}
