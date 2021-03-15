package com.shangbaishuyao.hadoop.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Desc:  分组比较器 ,即Reducer端进行分组 <br/>
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 11:44 2021/2/24
 *
 * 自定义分组比较需要继承WritableComparator ， 并重写compare(WritableComparable a , WritableComparable b )方法.
 *
 */

public class OrderWritableComparator extends WritableComparator{


	//写一个构造器  就是告诉他将来我分组比较的时候 我key的类型是什么  是否需要给你创建对象,true
	//这就表示我给OrderBean这个类型绑定了一个分组比较器.
	public OrderWritableComparator() {
		super(OrderBean.class,true) ;
	}
	
	/**
	 * 分组比较:  只要OrderBean对象的order_id相同就认为是相同的key
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean)a ;
		OrderBean bBean = (OrderBean)b ;
		
		int result ; 
		
		if(aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1 ; 
		}else if (aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1; 
		}else {
			result = 0 ;
		}
		
		return result ;
	}
}
