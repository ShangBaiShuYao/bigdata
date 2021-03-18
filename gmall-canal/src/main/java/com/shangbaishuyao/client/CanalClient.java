package com.shangbaishuyao.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.shangbaishuyao.constants.GmallConstants;
import com.shangbaishuyao.utils.KafkaSender;
import java.net.InetSocketAddress;
import java.util.List;
/**
 * Desc: 编写canal发送至kafka <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 0:15 2021/3/17
 */
public class CanalClient {
    public static void main(String[] args) {
        //获取Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(
                "hadoop102",  //主机名
                11111),           //端口号
                "example",  //连接的实例
                "",          //用户名
                "");         //密码

        //长轮询连接
        while (true) {
            //连接canal
            canalConnector.connect();
            //订阅监控的数据库
            canalConnector.subscribe("gmall.*");
            //抓取数据,一次抓取多少条
            Message message = canalConnector.get(100);
            //判断当前是否有数据更新
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一下！！！");
                try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            } else {
                //解析message
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断当前的Entry类型,过滤掉类似于事务开启与关闭的操作
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        //解析entry获取表名
                        String tableName = entry.getHeader().getTableName();
                        try {
                            //获取并解析StoreValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            //行数据集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //INSERT UPDATE ALTER DELETE
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //处理数据并发送至Kafka
                            handler(tableName, rowDatasList, eventType);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * canal要监控那几张表 <br/>
     * 这里面我们只要新增的部分
     * INSERT 表示我只要插入语句
     *
     * CanalEntry.EventType.INSERT.equals(eventType):   INSERT表示数据库里面新增的数据集
     * CanalEntry.EventType.UPDATE.equals(eventType):   UPDATE表示数据库里面变化的数据集,比如修改了数据库里面的某个字段
     *
     * @param tableName
     * @param rowDatasList
     * @param eventType
     */
    private static void handler(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {
            //监控新增的数据集
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) { //INSERT 表示我只要插入语句
            //将数据发送至order_info主题 订单数据主题
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_TOPIC);
            //监控新增的数据集
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //将数据发送至order_detail主题  订单详情主题
            sendToKafka(rowDatasList, GmallConstants.GMALL_ORDER_DETAIL_TOPIC);
            //监控新增及变化的数据集
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            //将数据发送至user_info主题  用户信息主题
            sendToKafka(rowDatasList, GmallConstants.GMALL_USER_TOPIC);
        }
    }

    /**
     * 发送至kafka
     * @param rowDatasList
     * @param topic
     */
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        //解析rowDatasList
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            //解析每一行数据
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //发送至Kafka
            KafkaSender.send(topic, jsonObject.toString());
        }
    }
}
