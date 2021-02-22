package com.shangbaishuyao.hadoop.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Desc:
 *  自定义分区需要继承partitioner类 , 重写getPartition方法
 *
 *  手机号为key , flowBean为value
 *
 *
 *   需求: 我希望 136 放在0号分区, 137放在1号分区, 以此类推...到139放在3号分区..至于其他的放到4号分区.<br/>
 *
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 16:31 2021/2/22
 */
public class phoneNumberPartitioner  extends Partitioner<FlowBean, Text>{

    public int getPartition(FlowBean key, Text value, int numPartitions) {

        //获取手机号的前三位
        String preStr = value.toString().substring(0,3);

        int partition;

        if("136".equals(preStr)) {
            partition = 0 ;
        }else if ("137".equals(preStr)) {
            partition = 1 ;
        }else if ("138".equals(preStr)) {
            partition = 2 ;
        }else if ("139".equals(preStr)) {
            partition = 3 ;
        }else {
            partition = 4 ;
        }

        return partition;
    }
}

