package com.shangbaishuyao.hadoop.reduceJoin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Desc: 订单对象 <br/>
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 21:51 2021/2/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderBean  implements Writable {
	
	private String order_id ;  // 订单id
	
	private String p_id ;  // 产品id
	
	private Integer amount ; // 数量
	
	private String pname ;  // 产品名
	
	private String title ; // 标签， 区分当前对象来至于哪个文件


	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(p_id);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(title);
		
	}

	public void readFields(DataInput in) throws IOException {
		order_id = in.readUTF();
		p_id = in.readUTF();
		amount= in.readInt();
		pname  = in.readUTF();
		title = in.readUTF();
	}

	@Override
	public String toString() {
		return  order_id  + "\t" + pname + "\t" + amount  ; 
	}
	
	
}
