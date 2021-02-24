package com.shangbaishuyao.hadoop.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/24
 * @Author: 上白书妖
 * @Date: 22:32 2021/2/24
 *
 * map join: 适合于一张小表  一张大表 的情景. 
 * 
 * 把小表的数据提前加载到内存中，缓存起来.  然后对于大表的数据，每读取一个kv 就与内存中小表的数据进行一次Join,
 * 得到join后的结果，将数据写出. 
 * 
 * 如果业务需求不是很复杂的情况，所有的操作可以直接在Map端完成. 省略Reduce. 
 *
 */

public class MapJoinMapper  extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	private Text k = new Text();
	
	private Map<String,String> pdMap = new HashMap<String, String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//计数器
		context.getCounter("Map Join", "setup").increment(1);
		
		//获取缓存的数据
		URI[] cacheFiles = context.getCacheFiles();
		//获取当前的缓存文件
		URI  currentCacheFile = cacheFiles[0];
		//读取文件
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		FSDataInputStream fis = fs.open(new Path(currentCacheFile));
		
		BufferedReader  reader  = new BufferedReader(new InputStreamReader(fis));
		
		String line ; 
		while(StringUtils.isNotEmpty( line = reader.readLine())) {
			String [] splits = line.split("\t");
			pdMap.put(splits[0], splits[1]);
		}
	
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 计数器
		context.getCounter("Map Join", "map").increment(1);
		
		//获取一行
		String line = value.toString();
		//切割
		String [] splits = line.split("\t");
		
		//获取pname
		String pname = pdMap.get(splits[1]);
		
		//拼接数据
		line = splits[0] +"\t" + pname + "\t" + splits[2];
		
		k.set(line);
		
		context.write(k, NullWritable.get());
	}
}
