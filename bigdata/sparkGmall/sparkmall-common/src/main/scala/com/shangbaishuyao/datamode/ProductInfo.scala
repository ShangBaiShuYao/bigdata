package com.shangbaishuyao.datamode

/***
  * Desc: 产品表 <br/>
  * create by shangbaishuyao on 2021/3/9
  * @Author: 上白书妖
  * @Date: 14:34 2021/3/9
  * @param product_id   商品的ID
  * @param product_name 商品的名称
  * @param extend_info  商品额外的信息
  */

case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String)

