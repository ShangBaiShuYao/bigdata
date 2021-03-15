package com.shangbaishuyao.hadoop.smallInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
/**
 * Desc:
 * 	自定义InputFormat需要 继承FileInputFormat<k,v>类.
 * 	 1. 是否可切分     isSplitable()
 *   2. 如何生成切片信息     getSplits()
 *   3. 创建RecordReader   createRecordReader()
 *
 *
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:29 2021/2/22
 */

public class SmallFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     *  我们的需求是将一个文件中的所有内容作为Value。
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 为当前的InputFormat 创建RecordReader对象，用于数据的读取
     */
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        SmallFileRecordReader recordReader = new SmallFileRecordReader();

        //调用初始化方法
        recordReader.initialize(split,context);

        return recordReader;

    }
}
