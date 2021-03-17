package com.shangbaishuyao.gmalllogger.Controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Desc: 将数据发送到kafka <br/>
 * create by shangbaishuyao on 2021/3/14
 * @Author: 上白书妖
 * @Date: 0:47 2021/3/14
 */
@Slf4j
@RestController
public class LoggerController {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String logger(@RequestParam("logString")String logString){
        System.out.println(logString);
        //添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //打印到控制台并输出至文件
        String tsLogStr = jsonObject.toString();
        log.info(tsLogStr);

        //发送至Kafka，判断发送至哪个主题
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.GMALL_START_TOPIC, tsLogStr);
        } else {
            kafkaTemplate.send(GmallConstants.GMALL_EVENT_TOPIC, tsLogStr);
        }
        return "success";
    }
}
