package com.shangbaishuyao.hadoop.NLineInputFormat;

import com.shangbaishuyao.hadoop.wordCount.WordCountDriver;
import com.shangbaishuyao.hadoop.wordCount.WordCountMapper;
import com.shangbaishuyao.hadoop.wordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/*
 * Desc: 驱动类 <br/>
 *
 *
 * create by shangbaishuyao on 2021/2/21
 * @Author: 上白书妖
 * @Date: 16:52 2021/2/21
 */
public class NLineInputFormatDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/NLineInputFormat/NLineInputFormat", "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/NLineInputFormat/out" };

        //1. 创建一个Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2. 关联jar
        job.setJarByClass(WordCountDriver.class);
        //3. 关联Mapper 和 Reuder
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4 设置Mapper输出的key 和 value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5. 设置最终输出的key  和  Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6. 设置输入和 输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //设置使用NLineInputFormat
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        //7. 提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
