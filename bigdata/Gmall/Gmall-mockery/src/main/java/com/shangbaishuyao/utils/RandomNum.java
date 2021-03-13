package com.shangbaishuyao.utils;

import java.util.Random;
/**
 * Desc: 随机数 <br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 19:27 2021/3/13
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}