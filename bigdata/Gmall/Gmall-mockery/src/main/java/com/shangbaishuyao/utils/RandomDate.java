package com.shangbaishuyao.utils;

import java.util.Date;
import java.util.Random;
/**
 * Desc: 随机日期 <br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 19:23 2021/3/13
 */
public class RandomDate {

    Long logDateTime =0L;
    int maxTimeStep=0 ;

    public RandomDate (Date startDate , Date  endDate,int num) {
        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();
    }

    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }
}