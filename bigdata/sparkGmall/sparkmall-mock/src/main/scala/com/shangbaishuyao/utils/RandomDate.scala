package com.shangbaishuyao.utils

import java.util.Date

import scala.util.Random

/**
 * Desc: 随机日期工具类 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 14:37 2021/3/9
 */
object RandomDate {
  def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
    val randomDate = new RandomDate()
    val avgStepTime: Long = (endDate.getTime - startDate.getTime) / step
    randomDate.maxTimeStep = avgStepTime * 2
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }

  class RandomDate {
    var lastDateTime = 0L
    var maxTimeStep = 0L

    def getRandomDate: Date = {
      val timeStep: Int = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep

      new Date(lastDateTime)
    }
  }
}