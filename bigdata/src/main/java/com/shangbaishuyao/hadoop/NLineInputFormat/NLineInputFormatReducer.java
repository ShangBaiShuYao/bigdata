package com.shangbaishuyao.hadoop.NLineInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Desc:
 * 自定义的Reduce需要继承Reduce类. 重写reduce方法
 *
 *
 * KEYIN  表示Reducer输入的Key. 跟Mapper输出的Key类型一致
 * VALUEIN 表示Recuder输入的Value,跟Mapper输出的Value类型一致
 *
 * KEYOUT   表示Reducer输出的key
 * VALUEOUT 表示Reduer输出的Value
 *
 * create by shangbaishuyao on 2021/2/21
 * @Author: 上白书妖
 * @Date: 16:53 2021/2/21
 */
public class NLineInputFormatReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable  v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 1 汇总
        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        // 2 输出
        context.write(key, v);
    }
}
