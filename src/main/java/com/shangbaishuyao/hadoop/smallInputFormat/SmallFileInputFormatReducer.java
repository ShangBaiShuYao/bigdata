package com.shangbaishuyao.hadoop.smallInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:36 2021/2/22
 */
public class SmallFileInputFormatReducer  extends Reducer<Text, BytesWritable, Text, BytesWritable>{

    protected void reduce(Text key, Iterable<BytesWritable> values,
                          Reducer<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {

        //写出
        context.write(key, values.iterator().next());
    }
}
