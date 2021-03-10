package com.shangbaishuyao.datamode

/**
  * Desc: 用户信息表 <br/>
  * create by shangbaishuyao on 2021/3/9
  * @Author: 上白书妖
  * @Date: 14:34 2021/3/9
  * @param user_id      用户的ID
  * @param username     用户的名称
  * @param name         用户的名字
  * @param age          用户的年龄
  * @param professional 用户的职业
  * @param gender       用户的性别
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    gender: String)