package com.shangbaishuyao.hadoop.Writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc: 继承map()方法 <br/>
 *
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 16:18 2021/2/20
 */
public class FlowCountMapper  extends Mapper<LongWritable, Text,Text,FlowBean> {
    FlowBean v = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        // 7 	13560436666	120.196.100.99		1116		 954			200
        String line = value.toString();

        //切分
        String [] splits = line.split("\t");

        //key
        String keyString  = splits[1];

        //value
        v.setUpFlow(Long.parseLong(splits[splits.length-3]));
        v.setDownFLow(Long.parseLong(splits[splits.length-2]));
        v.setSumFlow(Long.parseLong(splits[splits.length-3]) + Long.parseLong(splits[splits.length-2]));


        //写出
        k.set(keyString);
        context.write(k, v);
    }
}
