package com.shangbaishuyao.gracefully

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
/**
 * Desc: 优雅的关闭 <br/>
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 13:57 2021/3/12
 */
class MonitorStop(ssc: StreamingContext) extends Runnable {
  //重写run方法
  override def run(): Unit = {
    //获取文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "bigdata")
    //一个死循环,只要你程序不停,我监控就不能停
    while (true) {
      try
        Thread.sleep(20000) //每个20秒监控一次,去访问是否存在
      catch {
        case e: InterruptedException => e.printStackTrace()
      }
      //获取当前StreamingContext的一个状态
      val state: StreamingContextState = ssc.getState
      //同时再获取这个目录是否存在,生产环境当中要写清楚这是属于哪一类任务当中的,条件要写清楚
      val bool: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopSpark"))
      //当这个目录存在
      if (bool) {
        //判断我获取的状态是否是活跃状态
        if (state == StreamingContextState.ACTIVE) {
          //优雅的关闭
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}

