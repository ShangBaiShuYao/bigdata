package com.shangbaishuyao.hadoop.WritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Desc: 继承map()方法 <br/>
 *
 *
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 16:18 2021/2/20
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,  FlowBean, Text> {

    Text v  = new Text();
    FlowBean k = new FlowBean();

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,  FlowBean, Text>.Context context)
            throws IOException, InterruptedException {
        //获取一行数据
        // 7 	13560436666	120.196.100.99		1116		 954			200
        String line = value.toString();

        //切分
        String [] splits = line.split("\t");

        //key
        String keyString  = splits[1];

        //value
        k.setUpFlow(Long.parseLong(splits[splits.length-3]));
        k.setDownFLow(Long.parseLong(splits[splits.length-2]));
        k.setSumFlow(Long.parseLong(splits[splits.length-3]) + Long.parseLong(splits[splits.length-2]));


        //写出
        v.set(keyString);

        context.write(k,v );
    }
}
