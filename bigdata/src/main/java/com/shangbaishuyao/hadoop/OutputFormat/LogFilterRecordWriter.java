package com.shangbaishuyao.hadoop.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 16:50 2021/2/24
 *
 * 自定义的RecordWriter， 需要继承RecordWriter
 * 
 * 
 * 需求: 将包含"shangbaishuyao"的日志写到 d:/shangbaishuyao.log, 其他的日志写到 d:/other.log
 *
 */
public class LogFilterRecordWriter  extends RecordWriter<Text, NullWritable>{
	
	private String  shangbaishuyaoPath = "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/OutputFormat/result/shangbaishuyao.log";
	private String  otherPath = "H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/OutputFormat/result/other.log";
	
	private FSDataOutputStream shangbaishuyaoOut  ;
	private FSDataOutputStream otherOut   ; 

	public LogFilterRecordWriter(TaskAttemptContext context) {
		try {
			//获取文件系统对象
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			//创建两个输出流
			 shangbaishuyaoOut  = fs.create(new Path(shangbaishuyaoPath));
			 otherOut = fs.create(new Path(otherPath));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
	
	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		//获取一条日志
		String log = key.toString();
		
		if(log.contains("shangbaishuyao")) {
			shangbaishuyaoOut.writeBytes(log);
		}else {
			otherOut.writeUTF(log);
		}
		
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(shangbaishuyaoOut);
		IOUtils.closeStream(otherOut);
	}

}
