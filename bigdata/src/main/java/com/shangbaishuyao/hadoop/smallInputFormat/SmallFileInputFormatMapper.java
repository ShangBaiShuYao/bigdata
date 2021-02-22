package com.shangbaishuyao.hadoop.smallInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
 * Desc:
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:36 2021/2/22
 */
public class SmallFileInputFormatMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {

    protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
