package com.shangbaishuyao.hadoop.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Desc: 订单对象 <br/>
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 11:17 2021/2/24
 */
public class OrderBean  implements WritableComparable<OrderBean> {
	
	private  Integer  order_id ;  //订单id
	
	private  Double   price ; // 价格
	
	public OrderBean() {
	}

	public Integer getOrder_id() {
		return order_id;
	}

	public void setOrder_id(Integer order_id) {
		this.order_id = order_id;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}


	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id );
		out.writeDouble(price);
	}


	public void readFields(DataInput in) throws IOException {
		order_id = in.readInt();
		price = in.readDouble();
	}

	/**
	 * 排序比较:
	 * 	 根据订单id升序， 如果订单id相同，再按照价格降序。
	 */
	public int compareTo(OrderBean o) {
		int result ;
		if(this.order_id  > o.getOrder_id()) {
			result = 1 ;
		}else if (this.order_id < o.getOrder_id()) {
			result = -1 ;
		}else {
			if(this.getPrice() > o.getPrice()) {
				result = -1 ;
			}else if (this.getPrice() < o.getPrice()) {
				result =1 ;
			}else {
				result = 0 ;
			}
		}

		return result ;
	}


	public String toString() {
		return order_id + "\t"+ price;
	}
}
