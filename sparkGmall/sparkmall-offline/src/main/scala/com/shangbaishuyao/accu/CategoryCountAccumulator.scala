package com.shangbaishuyao.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * Desc: 自定义累加器 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 17:01 2021/3/9
 */
class CategoryCountAccumulator extends AccumulatorV2 [String,mutable.HashMap[String,Long]]{

  //定义hashMap存放累加器中的数据
  private val categoryCountMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  //判空
  //判断当前累加器是否空 ; 我们判断当前累加器是否为空,实际上就是判断他的属性是否为空
  override def isZero: Boolean = categoryCountMap.isEmpty

  //复制
  //复制一个新的累加器,这个方法做什么用的呢? 因为我们只是在Driver端定义好了累加器,我是不是要发给很多的Executor呀,他要复制发给其他的Executor
  override def copy(): AccumulatorV2[String, mutable.HashMap[String,Long]] = {
    new CategoryCountAccumulator
  }

  //重置
  //重置累加器, 就是当你复制好了一个累加器之后,他会判断你当前累加器是否为空,如果不为空他就要重置
  override def reset(): Unit = {
    categoryCountMap.clear()
  }

  //区内累加数据, 这个add是在一个一个Task里面执行的
  //给累加器添加值的
  override def add(v: String): Unit = {

    //有可能(click,16)我们传他进来这个东西在当前的map中有两种可能,一种是存在,一种是不存在.
    //你第一次来的时候点击16的这条数据, 第一条肯定是不存在的, 非第一条就已经是存在了,所以两种情况都要考虑到
    //假如存在,你的(click,16)要将16变成17. 假如不存在则变成(click,1)这种情况
    categoryCountMap(v)= {
      categoryCountMap.getOrElse(v,0L) + 1L   //getOrElse(v,0L)获取原本的, 如果有则+1L 变成(click,16L)写进去
    }
  }

  //区间累加数据
  //结果发回来之后,由Driver里面去做累加
  //将多个分区的数据进行合并
  //我们要把数据放到多个Executor里面去执行,最后返回的结果是不是多个Executor汇总的结果呀,这个merge就是说你要把多个Executor里面的值给我发回来,发回来之后将多个分区的数据进行合并
  //就是将other这个内容合并到当前的List里面
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String,Long]]): Unit = {
    //不能直接map覆盖, 一个map里面有(click,15), 另一个里面有map(click,16) 应该得到的结果是(click,31)
    //值取出来做累加
    //现在返回值是Unit,所以要求你把other追加到当前的categoryCountMap(v)里面
    //categoryCountMap是集合  foldLeft:折叠,这是scala里面的  other.value是将另外一个map给他放进来
//    categoryCountMap.foldLeft(other.value){
//        //  otherValue 是other.value
//        //  (category,count) 是categoryCountMap里面的每一个元素,一次一次拿过来的
//      case (otherMap,(category,count)) =>{
//        categoryCountMap(category) = otherMap.getOrElse(category,0L) + count
//        //返回
//        categoryCountMap
//      }
//    }
    other.value.foreach {
      case (category, count) =>{
        categoryCountMap(category) = categoryCountMap.getOrElse(category, 0L) + count
      }
    }
  }

  //返回值
  //获取累加器中的数据
  override def value: mutable.HashMap[String,Long] = categoryCountMap
}
