package com.shangbaishuyao.hadoop.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 21:52 2021/2/24
 *
 *  两个文件:  order.txt    pd.txt  <br/>
 *  
 *  将两个文件中的每一条数据都封装成一个Order对象, 包含5个属性,  4个为订单的信息: order_id, p_id, amount, pname , 1 个为标签 <br/>
 *  order : 1001	01	   1
 *     pd :         01	  小米
 * 
 *
 */
public class ReduceJoinMapper  extends  Mapper<LongWritable, Text, Text, OrderBean> {
	
	private String fileName ;
	private Text k  = new Text();
	private OrderBean  v  = new OrderBean();
	
	/**
	 * 获取到当前MapTask所处理的切片对象，以确定当前处理的是哪个文件
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//获取到当前的切片对象
		InputSplit inputSplit = context.getInputSplit() ; 

		//强制转换成FileSplit
		FileSplit fileSplit  = (FileSplit)inputSplit;
		fileName = fileSplit.getPath().getName() ;
				
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取一行
		String line = value.toString();
		//切割
		String [] splits = line.split("\t");
		//封装对象
		if(fileName.contains("order")) {
			//订单表的数据
			//封装key
			k.set(splits[1]);
			//封装Value
			v.setOrder_id(splits[0]);
			v.setP_id(splits[1]);
			v.setAmount(Integer.parseInt(splits[2]));
			v.setTitle("order");
			
			//pname
			v.setPname("");
		}else {
			//产品表的数据
			
			//封装key
			k.set(splits[0]);
			
			//封装value
			v.setP_id(splits[0]);
			v.setPname(splits[1]);
			v.setTitle("pd");
			
			//order_id  amount 
			v.setOrder_id("");
			v.setAmount(0);
		}
		//写出
		context.write(k, v);
	
	}
}








