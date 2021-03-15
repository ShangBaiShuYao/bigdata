package com.shangbaishuyao.hadoop.FindBlogFriends.FindOneTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/25
 * @Author: 上白书妖
 * @Date: 21:30 2021/2/25
 */
public class OneShareFriendsReducer extends Reducer<Text, Text, Text, Text>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        //1 拼接
        for(Text person: values){
            sb.append(person).append(",");
        }

        //2 写出
        context.write(key, new Text(sb.toString()));
    }
}
