package com.shangbaishuyao.datamode

/**
  * Desc: 城市信息表 <br/>
  * create by shangbaishuyao on 2021/3/9
  * @Author: 上白书妖
  * @Date: 14:33 2021/3/9
  * @param city_id     城市id
  * @param city_name   城市名称
  * @param area        城市所在大区
  */
case class CityInfo (city_id:Long,
                     city_name:String,
                     area:String)