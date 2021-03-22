package com.shangbaishuyao.bean

/**
 * 订单详情
 * @param id
 * @param order_id
 * @param sku_name
 * @param sku_id
 * @param order_price
 * @param img_url
 * @param sku_num
 */
case class OrderDetail( id:String ,
                        order_id: String,
                        sku_name: String,
                        sku_id: String,
                        order_price: String,
                        img_url: String,
                        sku_num: String)