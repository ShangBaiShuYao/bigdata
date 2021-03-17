package com.shangbaishuyao.sendTheLog;

import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;
/**
 * Desc: 发送日志工具类 <br/>
 * LogUploader：通过http方法发送到采集系统的web端口
 *
 * 这个只是负责传过来,传来了之后将日志发送出去, 但是日志如何生成的我就不知道了.
 *
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 19:31 2021/3/13
 */
public class LogUploader {
    public static void sendLogStream(String log){
        try{
            //请求地址
            //不同的日志类型对应不同的URL
            URL url  =new URL("http://hadoop102/log");
            //本地测试打印
//            URL url  =new URL("http://127.0.0.1:8080/log");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //设置请求方式为post
            conn.setRequestMethod("POST");
            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");
            //允许上传数据
            conn.setDoOutput(true);
            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            System.out.println("upload" + log);
            //输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString="+log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}