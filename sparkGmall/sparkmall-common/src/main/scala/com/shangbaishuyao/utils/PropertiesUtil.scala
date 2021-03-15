package com.shangbaishuyao.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Desc: 配置信息工具类 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 14:35 2021/3/9
 */
object PropertiesUtil {

  def load(propertiesName:String): Properties ={

    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) , "UTF-8"))
    prop
  }

}