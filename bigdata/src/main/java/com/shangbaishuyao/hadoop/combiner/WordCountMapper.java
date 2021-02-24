package com.shangbaishuyao.hadoop.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/23
 * @Author: 上白书妖
 * @Date: 11:40 2021/2/23
 *
 * 自定义Mapper需要继承Mapper类，并重写map方法. 
 *
 * 4个泛型:
 * 	KEYIN:   输入到Mapper中key的类型 
 *  VALUEIN: 输入到Mapper中的value的类型
 *  
 *  KEYOUT:  Mapper输出的key的类型
 *  VALUEOUT: Mapper输出的value的类型
 *  
 *  
 * 分析的文件内容: 
 * 	atguigu atguigu
	ss ss
	cls cls
	jiao
	banzhang weihong
	xue
	hadoop
	
	
	当前WordCountMapper中 输入KV  与 输出 KV类型的解释:
	输入kv:	
		key: LongWritable  记录的是文件内容的偏移量,文件读取到的位置
		value: Text  表示文件中读取到的一行数据
	输出kv:
		key: Text  表示一个单词作为一个key.
		value: IntWritable  标识当前key对应的单词出现的次数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	//输出的key  和  Value
	Text k = new Text();
	
	IntWritable v = new IntWritable(1);
	
	/**
	 * 重写map方法
	 * 
	 * Context: 上下文对象.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1.获取一行数据    例如: atguigu atguigu
		String line = value.toString();
		
		
		//2.切分数据
		String []  splits =  line.split(" ");
		
		//3.循环输出kv
		for (String s : splits) {
			
			k.set(s);
			//输出
			context.write(k, v);
		}
		
	}
}




