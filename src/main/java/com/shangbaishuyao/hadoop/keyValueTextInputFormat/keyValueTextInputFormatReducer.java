package com.shangbaishuyao.hadoop.keyValueTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:14 2021/2/22
 */
public class keyValueTextInputFormatReducer extends Reducer <Text, IntWritable,Text,IntWritable>{
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //汇总
        int sum = 0;
        for (IntWritable intWritable:values) {
            sum += intWritable.get();
        }

        v.set(sum);
        //写出
        context.write(key,v);
    }
}
