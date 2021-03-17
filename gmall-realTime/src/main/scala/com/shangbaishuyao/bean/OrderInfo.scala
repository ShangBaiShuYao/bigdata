package com.shangbaishuyao.bean

/**
 * 数据来源于数据库 order_info 这张表
 * 新增了var create_date: String, var create_hour: String 这两个字段.
 * 因为虽然表里面有create_time 但是他的格式是2021-03-16 14:21:21 年月日时分秒形式的
 * 我们要分时统计, 我们需要年月日一个字段,小时一个字段 这里我们新增字段
 * 收件人,手机号脱敏等等
 *
 * @param id
 * @param province_id
 * @param consignee
 * @param order_comment
 * @param consignee_tel
 * @param order_status
 * @param payment_way
 * @param user_id
 * @param img_url
 * @param total_amount
 * @param expire_time
 * @param delivery_address
 * @param create_time
 * @param operate_time
 * @param tracking_no
 * @param parent_order_id
 * @param out_trade_no
 * @param trade_body
 * @param create_date
 * @param create_hour
 */
case class OrderInfo(
                      id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String,
                      var create_hour: String)
