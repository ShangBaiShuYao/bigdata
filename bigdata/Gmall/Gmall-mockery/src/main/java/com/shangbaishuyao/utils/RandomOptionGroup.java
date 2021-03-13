package com.shangbaishuyao.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
/**
 * Desc: 直接使用权重放数据了 <br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 19:26 2021/3/13
 */
public class RandomOptionGroup<T> {

    int totalWeight=0;

    List<RanOpt> optList=new ArrayList();

    public   RandomOptionGroup(RanOpt<T>... opts) {
        for (RanOpt opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i <opt.getWeight() ; i++) {
                optList.add(opt);
            }
        }
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }


    //测试
    public static void main(String[] args) {
        RanOpt[] opts= {new RanOpt("shangbaishuyao",20),new RanOpt("dsp",30),new RanOpt("dsx",50)};
        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);
        for (int i = 0; i <10 ; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }
    }

}