package com.shangbaishuyao;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
/**
 * Desc: 重写协处理器 <br/>
 * 协处理器的功能就是, 我在一个表里面添加数据,而协处理器则帮助你自己在另一个表里面也添加了数据,这就是协处理器的功能.
 * 比如: Phoenix里面,我建立全局索引表, 他根据我在原表里面插入的数据,自己往全局表里面插入了相关数据, 这就是协处理器做的<br/>
 *
 * BaseRegionObserver里面有许多的postXXX(),或者preXXX()方法.
 * 假入你给当前这个fruit这个表祖注册了一个协处理器, preput是什么意思呢?他就会在你往当前这张表
 * 里面插入数据之前回调这个方法里面的内容,postput就是在你往这张表里面插入数据之后,调用这个postput方法,做观察的,观察你整个的对于他的操作
 *
 * 这个需要打包扔到集群才能作用的.
 *
 * 假如说我给一张表去建立索引. 他把一个信息记录下来.他自己把这个协处理器
 * 封装好,去写好,写往之后呢,它记录你要写几个列,假如我的Phoenix原来的
 * student表里面有id, 是row_key. 我建立二级索引是name这个列.我将name建立个索引就相当于
 * 给这张表额外注册了一个协处理器.同时建立一张索引表.我往主表studnet里面插入数据,他会通过
 * 协处理器的方式往索引表 里面也放一条数据.读数据时,只要有索引表,他会先读索引表,索引表数据全则主表就不读了
 *
 * create by shangbaishuyao on 2021/3/16
 * @Author: 上白书妖
 * @Date: 1:23 2021/3/16
 */
public class FruitTableCoprocessor extends BaseRegionObserver {
    //postput就是在你往这张表里面插入数据之后,调用这个postput方法,做观察的
    //前面的操作是我要给表插入一个列名为name的字段的值为dsp. 我需要将dsp插入进去, 那个这东西我是将他封装成Put对象来完成的.
    //所以在我插入数据之后,我可以从Put对象中的到这个内容(dsp)
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        //获取表对象
        Table table = connection.getTable(TableName.valueOf("fruit"));
        //插入数据
        table.put(put);
        //关闭资源
        table.close();
        connection.close();
    }
}