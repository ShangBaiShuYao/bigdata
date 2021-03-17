package com.shangbaishuyao.bean
/**
 * Desc: 预警日志实例类 <br/>
 * 这个日志最终是要保留的
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 11:51 2021/3/17
 */
case class CouponAlertInfo(mid:String,
                          //用户id
                           uids:java.util.HashSet[String],
                          //领券的过程中产生的商品id
                           itemIds:java.util.HashSet[String],
                          //领券的人行为,除了领券是否还有其他行为
                           events:java.util.List[String],
                           ts:Long)
