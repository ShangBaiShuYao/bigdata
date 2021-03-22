package com.shangbaishuyao.bean

import java.text.SimpleDateFormat
import java.util

import lombok.{AllArgsConstructor, Data, NoArgsConstructor}

/**
 * 销售详情
 * 这是最终三表join后最终呈现内容的所有字段样例类
 * @param order_detail_id
 * @param order_id
 * @param order_status
 * @param create_time
 * @param user_id
 * @param sku_id
 * @param user_gender
 * @param user_age
 * @param user_level
 * @param sku_price
 * @param sku_name
 * @param dt
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
case class SaleDetail(
                       var order_detail_id: String = null,
                       var order_id: String = null,
                       var order_status: String = null,
                       var create_time: String = null,
                       var user_id: String = null,
                       var sku_id: String = null,
                       var user_gender: String = null,
                       var user_age: Int = 0,
                       var user_level: String = null,
                       var sku_price: Double = 0D,
                       var sku_name: String = null,
                       var dt: String = null) {

  //优化和并orderInfo和OrderDetail这两个主题里面的内容
  //因为orderInfo和OrderDetail是几乎同时产生的,虽然他两又先后产生
  //但是userInfo一定是比他们早产生出来的很长一段时间.可能你购买东西两条就注册了,但是没有购买东西,两天后才购买. 这时候才产生OrderInfo和orderDetail这两张表
  //比如你的京东在两年前就注册了,知道现在用户信息一直都没变更过,那你的userInfo这表
  //而下订单和订单详情几乎是同时产生的,而用户信息明显比他们量长.
  //所以对于orderInfo,orderDetail这两张几乎同时产生的表我们使用流的join
  //对于用户信息即userInfo,我们使用另外的方式join
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  def mergeOrderInfo(orderInfo: OrderInfo): Unit = {
    //只要我们传过来的字段不为空,我们就直接替换原有字段数值
    if (orderInfo != null) {
      this.order_id = orderInfo.id
      this.order_status = orderInfo.order_status
      this.create_time = orderInfo.create_time
      this.dt = orderInfo.create_date
      this.user_id = orderInfo.user_id
    }
  }

  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {
    if (orderDetail != null) {
      this.order_detail_id = orderDetail.id
      this.sku_id = orderDetail.sku_id
      this.sku_name = orderDetail.sku_name
      this.sku_price = orderDetail.order_price.toDouble
    }
  }

  def mergeUserInfo(userInfo: UserInfo): Unit = {
    if (userInfo != null) {
      this.user_id = userInfo.id

      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: util.Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val betweenMs: Long = curTs - date.getTime
      val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L

      this.user_age = age.toInt
      this.user_gender = userInfo.gender
      this.user_level = userInfo.user_level
    }
  }
}

