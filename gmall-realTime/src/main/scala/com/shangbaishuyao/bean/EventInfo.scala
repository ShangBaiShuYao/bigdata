package com.shangbaishuyao.bean
/**
 * Desc: 事件日志类型的实例类 <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 11:50 2021/3/17
 */
case class EventInfo(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     evid:String,
                     pgid:String,
                     npgid:String,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long)