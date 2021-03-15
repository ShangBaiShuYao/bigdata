package com.shangbaishuyao.RDD.Partitioner

import org.apache.spark.Partitioner

/**
 * Desc: 自定义分区器 <br/>
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 15:29 2021/3/8
 */
class MyPartitions (numPartition:Int) extends Partitioner{

  //调用numPartitions这个方法获取分区器里面个数
  override def numPartitions: Int = numPartition

  //返回分区号, 这里只有一个key ,所以只能按照key进行分区. 但是如果这里可以传value进来我们就可以通过value来进行分区了
  //但是这里只能按照key进行分区, 也就是说,spark当中,只能按照key进行分区
  override def getPartition(key: Any): Int = {
    0
  }
}
