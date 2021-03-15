package com.shangbaishuyao.hadoop.compress.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.jupiter.api.Test;

/**
 * Desc: <p>测试压缩与解压缩</p>
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 10:27 2021/2/25
 *
 */
public class TestCompress {
	
	/**
	 * 解压缩: 使用一个支持压缩的流将数据读取 <p/>
	 */
	@Test
	public void testDeCompress() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs =  FileSystem.get(conf);
		
		//输入流
		//获取编解码器
		final CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/compress/out/JaneEyre.deflate"));
		
		CompressionInputStream fis = codec.createInputStream(fs.open(new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/compress/out/JaneEyre.deflate")));
		
		//输出流
		FSDataOutputStream fos = fs.create(new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/compress/JaneEyre.txt"));
		
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		//关闭
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		
	}
	
	
	/**
	 * 压缩:  使用一个支持压缩的流将数据写出 <p/>
	 */
	@Test
	public void testCompress() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs =  FileSystem.get(conf);
		//输入文件: D:\input\inputWord\JaneEyre.txt   
		FSDataInputStream fis  = fs.open(new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/compress/JaneEyre.txt"));
		//输出文件: D:\codec\JaneEyre.deflate
		
		//获取编解码器 
		String codecClassName = "org.apache.hadoop.io.compress.DefaultCodec" ;
		Class<?> codecClass = Class.forName(codecClassName);
		CompressionCodec codec  = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf) ; 
		//获取带压缩功能的输出流
		CompressionOutputStream fos = codec.createOutputStream(fs.create(new Path("H:/IDEA_WorkSpace/bigdata/bigdata/src/main/resources/MapCompress"+ codec.getDefaultExtension())));
		
		//流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		//关闭流
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		
	}
}
