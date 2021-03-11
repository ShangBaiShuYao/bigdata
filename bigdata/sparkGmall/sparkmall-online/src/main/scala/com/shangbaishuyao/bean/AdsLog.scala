package com.shangbaishuyao.bean

/**
  * 某个时间点 某个地区 某个城市 某个用户 某个广告
  * @param timestamp
  * @param area
  * @param city
  * @param userid
  * @param adid
  */
case class AdsLog(timestamp: Long, area: String, city: String, userid: String, adid: String)
