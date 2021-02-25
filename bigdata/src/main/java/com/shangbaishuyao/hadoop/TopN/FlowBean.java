package com.shangbaishuyao.hadoop.TopN;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Desc: 编写对象接收实体类 <br/>
 *
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 21:20 2021/2/25
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowBean implements WritableComparable<FlowBean>{

    private long upFlow;
    private long downFlow;
    private long sumFlow;


    //序列化和反序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }


    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void set(long downFlow2, long upFlow2) {
        downFlow = downFlow2;
        upFlow = upFlow2;
        sumFlow = downFlow2 + upFlow2;
    }

    public int compareTo(FlowBean bean) {

        int result;

        if (this.sumFlow > bean.getSumFlow()) {
            result = -1;
        }else if (this.sumFlow < bean.getSumFlow()) {
            result = 1;
        }else {
            result = 0;
        }
        return result;
    }
}