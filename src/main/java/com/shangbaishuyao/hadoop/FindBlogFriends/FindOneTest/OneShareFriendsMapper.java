package com.shangbaishuyao.hadoop.FindBlogFriends.FindOneTest;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * Desc:
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 21:29 2021/2/25
 */
public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        // 1 获取一行 A:B,C,D,F,E,O
        String line = value.toString();

        if (StringUtils.isNotBlank(line)){
        // 2 切割
        String[] fields = line.split(":");

        // 3 获取person和好友
        String person = fields[0];
        String[] friends = fields[1].split(",");

        // 4写出去
        for(String friend: friends){

            // 输出 <好友，人>
            context.write(new Text(friend), new Text(person));
            }
        }
    }
}
