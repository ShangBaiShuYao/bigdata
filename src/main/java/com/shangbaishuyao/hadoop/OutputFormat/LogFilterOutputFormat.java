package com.shangbaishuyao.hadoop.OutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Desc:
 *
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 16:50 2021/2/24
 *
 * 自定义的OutputFormat 需要继承 FileOutputFormat，并重写getRecordWriter方法.
 *
 */
public class LogFilterOutputFormat extends FileOutputFormat<Text, NullWritable> {
	
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		LogFilterRecordWriter  recordWriter = new LogFilterRecordWriter(job);
		
		return recordWriter;
	}
}
