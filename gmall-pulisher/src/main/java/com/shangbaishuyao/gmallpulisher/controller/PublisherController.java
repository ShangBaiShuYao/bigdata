package com.shangbaishuyao.gmallpulisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmallpulisher.bean.Option;
import com.shangbaishuyao.gmallpulisher.bean.State;
import com.shangbaishuyao.gmallpulisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * Desc: DAU 日活需求, GMV 交易额需求 <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 11:19 2021/3/17
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    /**
     * 通过前端传过来的日期查询数据 <br/>
     * @param date 日期
     * @return
     */
    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        //获取日活数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //获取单日GMV
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        ArrayList<Map> result = new ArrayList<>();

        //添加日活数据信息
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        result.add(dauMap);

        //添加新增用户信息
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        result.add(newMidMap);

        //添加交易额数据信息
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", orderAmountTotal);
        result.add(gmvMap);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hours")
    public String getRealTimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        //定义JSON对象封装结果数据
        JSONObject jsonObject = new JSONObject();
        if ("dau".equals(id)) {
            //获取传入时间的分时统计
            Map todayHourMap = publisherService.getDauTotalHourMap(date);
            jsonObject.put("today", todayHourMap);

            //将传入时间日期减一
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date yesterdayDate = DateUtils.addDays(sdf.parse(date), -1);
                String yesterdayStr = sdf.format(yesterdayDate);
                //获取传入时间日期减一日期的分时统计
                Map yesterdayHourMap = publisherService.getDauTotalHourMap(yesterdayStr);
                jsonObject.put("yesterday", yesterdayHourMap);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else if ("order_amount".equals(id)) {

            //获取今天的交易总额
            Map todayMap = publisherService.getOrderAmountHourMap(date);
            //将传入时间日期减一
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date yesterdayDate = DateUtils.addDays(sdf.parse(date), -1);
                String yesterdayStr = sdf.format(yesterdayDate);
                //获取昨天的交易总额
                Map yesterdayMap = publisherService.getOrderAmountHourMap(yesterdayStr);
                jsonObject.put("today", todayMap);
                jsonObject.put("yesterday", yesterdayMap);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return jsonObject.toString();
    }


    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage, @RequestParam("size") int size, @RequestParam("keyword") String keyword) {
        //创建Map用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        //查询ES
        Map saleDetail = publisherService.getSaleDetail(date, startpage, size, keyword);
        //获取数据
        Long total = (Long) saleDetail.get("total");
        Map genderMap = (Map) saleDetail.get("genderMap");
        Map ageMap = (Map) saleDetail.get("ageMap");
        List detailList = (List) saleDetail.get("detail");
        //创建集合用于存放性别以及年龄占比的数据
        ArrayList<State> states = new ArrayList<>();
        //解析genderMap["F"->50,"M"->10]=>["男"->,"女"->]
        Long femaleCount = (Long) genderMap.get("F");
        double femaleRatio = Math.round(femaleCount * 1000 / total) / 10D;
        double maleRatio = 100D - femaleRatio;
        Option maleOp = new Option("男", maleRatio);
        Option femaleOp = new Option("女", femaleRatio);
        ArrayList<Option> genderOpList = new ArrayList<>();
        genderOpList.add(maleOp);
        genderOpList.add(femaleOp);
        //将性别比例添加至states集合
        states.add(new State("用户性别占比", genderOpList));
        //解析ageMap
        Long lower20 = 0L;
        Long start20to30 = 0L;
        //遍历ageMap,取出每个年龄段的数据
        for (Object o : ageMap.keySet()) {
            Long age = Long.parseLong((String) o);
            Long ageCount = (Long) ageMap.get(o);
            if (age < 20) {
                lower20 += ageCount;
            } else if (age < 30) {
                start20to30 += ageCount;
            }
        }
        double lower20Ratio = Math.round(lower20 * 1000 / total) / 10D;
        double start20to30Ratio = Math.round(start20to30 * 1000 / total) / 10D;
        double upper30Ratio = 100D - lower20Ratio - start20to30Ratio;

        Option lower20Op = new Option("20岁以下", lower20Ratio);
        Option start20to30Op = new Option("20岁到30岁", start20to30Ratio);
        Option upper30Op = new Option("30岁及30岁以上", upper30Ratio);
        ArrayList<Option> ageOpList = new ArrayList<>();
        ageOpList.add(lower20Op);
        ageOpList.add(start20to30Op);
        ageOpList.add(upper30Op);

        //将年龄比例添加至states集合
        states.add(new State("用户年龄占比", ageOpList));

        result.put("total", total);
        result.put("stat", states);
        result.put("detail", detailList);
        return JSON.toJSONString(result);
    }
}
