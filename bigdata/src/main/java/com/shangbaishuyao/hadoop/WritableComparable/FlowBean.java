package com.shangbaishuyao.hadoop.WritableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Desc:
 *
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 15:56 2021/2/20
 */

// 1 实现writable序列化接口
/**
 * 上行流量
 *
 * 下行流量
 *
 * 总流量
 */
public class FlowBean  implements   WritableComparable<FlowBean>{

    private  Long upFlow ;   //上行流量

    private  Long downFLow ; //下行流量

    private  Long sumFlow ;  //总流量

    public FlowBean() {
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFLow() {
        return downFLow;
    }

    public void setDownFLow(Long downFLow) {
        this.downFLow = downFLow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFLow + "\t" + sumFlow;
    }

    /**
     * 序列化方法
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFLow);
        out.writeLong(sumFlow);
    }
    /**
     * 反序列化方法
     *
     * 注意: 序列化的顺序与反序列化的顺序一定要保证一致。
     */
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFLow = in.readLong();
        sumFlow = in.readLong();
    }

    /**
     * 比较的方法
     *
     * 按照总流量倒叙排序
     */
    public int compareTo(FlowBean o) {

        int result = 0  ;

        if(this.sumFlow > o.getSumFlow()) {
            result = -1 ;
        }else if (this.sumFlow < o.getSumFlow()) {
            result  = 1 ;
        }

        return result ;
    }
}
