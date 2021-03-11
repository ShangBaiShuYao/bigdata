package com.shangbaishuyao.UDF

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * Desc: 自定义数据源 <br/>
 * 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。
 * create by shangbaishuyao on 2021/3/11
 *
 * StorageLevel.MEMORY_ONLY 存储级别
 * @Author: 上白书妖
 * @Date: 15:35 2021/3/11
 */
class myReceiver(hostName:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    //开启新的线程
    new Thread(){
      //重写run方法
      override def run(): Unit = {
          receive()
        }
    }.start()
  }

  override def onStop(): Unit = {}


  //写接收数据,并发送给spark集群
  def receive() = {

    try {
      val socket = new Socket(hostName, port)
      //使用包装流包装一下,创建输入流
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      //读取一行数据
      var inputLine: String = reader.readLine()

      //将数据写给Spark集群,循环读给spark集群
      while (inputLine !=null && !isStopped()){
        store(inputLine) //保存发送 ,发送完数据没了,然后下面再次去读
        inputLine = reader.readLine()
      }
      reader.close()
      socket.close()
      restart("重启......")
    }catch {
      case e : java.net.ConnectException => restart("连接异常,重启......")
      case t : Throwable => restart("错误,重启......")
    }
  }
}
