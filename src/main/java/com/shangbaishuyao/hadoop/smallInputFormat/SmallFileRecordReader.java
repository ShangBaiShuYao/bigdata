package com.shangbaishuyao.hadoop.smallInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Desc: 自定义的 RecordReader必须要继承 RecordReader类。 <br/>
 *
 * 自定义我们要他进行按文件读取 <br/>
 *
 * create by shangbaishuyao on 2021/2/22
 * @Author: 上白书妖
 * @Date: 9:20 2021/2/22
 */

public class SmallFileRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit currentFileSplit ;   // 当前的切片对象
    //private TaskAttemptContext context ;   // 上下文对象

    private Configuration conf ;  // 当前Job的配置对象

    private  Text  currentKey  = new Text();  // 当前的key

    private BytesWritable currentValue = new BytesWritable(); // 当前的value

    private Boolean  flag  = true ;  // 标记是否有下个KV


    /**
     * 初始化
     *
     * InputSplit : 当前的切片对象
     *
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取当前的切片对象
        currentFileSplit = (FileSplit) split ;
        conf = context.getConfiguration();
    }

    /**
     * 是否有下一个Key 和 Value
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(flag) {
            // 给当前的key  和   value 赋值.
            //封装key
            String currentKeyString  = currentFileSplit.getPath().toString();
            currentKey.set(currentKeyString);

            //封装value
            //获取文件系统对象
            FileSystem fs = FileSystem.get(conf);
            //获取指向待读取文件的输入流
            FSDataInputStream fis  = fs.open(currentFileSplit.getPath()) ;
            //创建字节数据，存储读取到的所有字节
            byte []  currentFileToBytes =  new byte [(int)currentFileSplit.getLength()] ;
            //将待读取文件的所有字节读取到 字节数组中
            IOUtils.readFully(fis, currentFileToBytes, 0, currentFileToBytes.length);

            //封装Value
            currentValue.set(currentFileToBytes, 0, currentFileToBytes.length);

            flag = false ;

            return true ;
        }
        return false;
    }

    /**
     * 获取当前的Key
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    /**
     * 获取当前的Value
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    /**
     * 获取RecordReader读取的进度
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 可以关闭一些打开的资源
     */
    @Override
    public void close() throws IOException {

    }


}
