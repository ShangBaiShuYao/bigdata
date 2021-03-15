package com.shangbaishuyao.hadoop.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * Desc:
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 21:52 2021/2/24
 *
 * 
 * <p> 解释: 为了更加节省内存空间, 对于reduce方法中的key 和 value 对象所指向的内存空间是不会变的， 每次迭代变的是内存空间中的数据.  <p/>
 *
 */

public class ReduceJoinReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
	
	private  OrderBean   pd  = new OrderBean() ;
	
	@Override
	protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {

	    List<OrderBean> orders = new ArrayList<OrderBean>();
		
		for (OrderBean orderBean : values) {
			
			if("pd".equals(orderBean.getTitle())){
				try {
						BeanUtils.copyProperties(pd, orderBean);
					} catch (Exception e) {
				}
			}else {
				try {
						OrderBean order = new OrderBean();
						BeanUtils.copyProperties(order, orderBean);
						orders.add(order);
					} catch (Exception e) {
				}
			}
		}
		
		//join
		for (OrderBean order : orders) {
			order.setPname(pd.getPname());
			
			//写出
			context.write(order, NullWritable.get());
		}
	
	}
}
