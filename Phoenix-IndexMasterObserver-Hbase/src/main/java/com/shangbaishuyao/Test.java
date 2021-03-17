package com.shangbaishuyao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;
/**
 * Desc: 创建表<br/>
 *
 * 创建fruit这个表, 同时添加了协处理器 <br/>
 *
 * create by shangbaishuyao on 2021/3/16
 * @Author: 上白书妖
 * @Date: 1:22 2021/3/16
 */
public class Test {
    public static void main(String[] args) throws IOException{
        //创建配置信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("fruit"));
        //添加列族信息
        tableDescriptor.addFamily(new HColumnDescriptor("info"));
        //注册协处理器
        tableDescriptor.addCoprocessor("com.shangbaishuyao.FruitTableCoprocessor");
        //创建表
        admin.createTable(tableDescriptor);
        //关闭连接
        admin.close();
        connection.close();
    }
}