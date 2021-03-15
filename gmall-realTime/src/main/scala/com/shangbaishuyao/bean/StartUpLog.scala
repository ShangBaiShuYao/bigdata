package com.shangbaishuyao.bean
/**
 * Desc: 启动日志样例类 gmall_start <br/>
 *
 * 将每一条json格式转换为样例类
 * {
 * "area":"guangdong",
 * "uid":"264",
 * "os":"andriod",
 * "ch":"baidu",
 * "appid":"gmall2019",
 * "mid":"mid_487",
 * "type":"startup",
 * "vs":"1.1.3"
 * }
 *
 * create by shangbaishuyao on 2021/3/14
 *
 * @Author: 上白书妖
 * @Date: 19:29 2021/3/14
 */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)

