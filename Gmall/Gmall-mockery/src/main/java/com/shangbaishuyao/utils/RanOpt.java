package com.shangbaishuyao.utils;
/***
 * Desc: 根据权重的方式 <br/>
 * 比如: 根据权重的方式生成数据. 比如张三和李四, 由四十个张三和六十个李四去获取这个值,就是一个比例问题<br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 19:24 2021/3/13
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}