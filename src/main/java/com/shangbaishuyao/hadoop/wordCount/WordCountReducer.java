package com.shangbaishuyao.hadoop.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Desc:
 *
 * 自定义的Reduce需要继承Reduce类. 重写reduce方法
 *
 * KEYIN  表示Reducer输入的Key. 跟Mapper输出的Key类型一致
 * VALUEIN 表示Recuder输入的Value,跟Mapper输出的Value类型一致
 *
 * KEYOUT   表示Reducer输出的key
 * VALUEOUT 表示Reduer输出的Value
 *
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 15:35 2021/2/20
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable  v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //一组数据:atguigu 1
        //		 atguigu 1

        int sum = 0 ;

        for (IntWritable value : values) {

            sum += value.get() ;
        }

        //写出
        v.set(sum);
        context.write(key, v);

    }
}


