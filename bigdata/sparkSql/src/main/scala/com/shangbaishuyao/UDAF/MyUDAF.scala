package com.shangbaishuyao.UDAF

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 * Desc: 自定义函数求计算平均数 <br/>
 * create by shangbaishuyao on '2021/3/10'
 *
 * @Author: 上白书妖
 * @Date: 16:22 '2021/3/10'
 */
class MyUDAF extends UserDefinedAggregateFunction{
  //定义输入类型,即定义输入的类型
  override def inputSchema: StructType = StructType(StructField("input",IntegerType)::Nil)

  //定义中间缓存的数据类型
  override def bufferSchema: StructType = StructType(StructField("sum",IntegerType)::StructField("count",IntegerType)::Nil)

  //输出数据类型, 两个int出来可能是doubleType
  override def dataType: DataType = DoubleType

  //函数稳定性
  override def deterministic: Boolean = true

  //中间缓存的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
      buffer(1) = 0
  }

  //区内累加数据, 就是将input中的数据加到buffer 现在buffer经过上面初始化是(0,0),input过来变成(10,1) 10加在第一位,1是自增1的
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getInt(0)+input.getInt(0)
    }
    buffer(1) = buffer.getInt(1) + 1
  }

  //区间累加数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
       buffer1(0) = buffer1.getInt(0)+buffer2.getInt(0)
       buffer1(1) = buffer1.getInt(1)+buffer2.getInt(1)
  }

  //最终计算
  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0) / buffer.getInt(1).toDouble
  }
}
