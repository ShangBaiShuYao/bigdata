package com.shangbaishuyao.hadoop.InvertedIndex.SecondTreatment;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc: 第二次处理，编写TwoIndexMapper类
 *
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 21:02 2021/2/25
 */

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取1行数据
        String line = value.toString();

        // 2用“--”切割
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);

        // 3 输出数据
        context.write(k, v);
    }
}