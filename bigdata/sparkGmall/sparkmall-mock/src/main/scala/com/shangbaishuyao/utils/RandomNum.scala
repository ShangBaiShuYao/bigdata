package com.shangbaishuyao.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Desc: 随机数工具类 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 14:37 2021/3/9
 */
object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean): String = {

    var str = ""
    if (canRepeat) {
      val buffer = new ListBuffer[Int]
      while (buffer.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        buffer += randoNum
        str = buffer.mkString(delimiter)
      }

    } else {
      val set = new mutable.HashSet[Int]()
      while (set.size < amount) {
        val randoNum: Int = fromNum + new Random().nextInt(toNum - fromNum)
        set += randoNum
        str = set.mkString(delimiter)
      }
    }
    str
  }

  def main(args: Array[String]): Unit = {
    println(RandomNum.multi(1, 10, 5, ",", canRepeat = false))
  }

}
