package com.shangbaishuyao.hadoop.TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Desc: 驱动类 <br/>
 *
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 21:21 2021/2/25
 */
public class TopNDriver {

    public static void main(String[] args) throws Exception {

        args  = new String[]{"H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/TopN/data","H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/TopN/out"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(TopNDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
