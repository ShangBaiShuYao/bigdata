package com.shangbaishuyao.hadoop.WritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Desc: 继承Reduce()方法 <br/>
 *
 * create by shangbaishuyao on 2021/2/20
 * @Author: 上白书妖
 * @Date: 16:18 2021/2/20
 */
public class FlowCountReducer  extends Reducer< FlowBean,Text, Text, FlowBean>{

    Text k = new Text();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {

        for (Text text : values) {
            k.set(text);

            context.write(k, key);
        }
    }
}
