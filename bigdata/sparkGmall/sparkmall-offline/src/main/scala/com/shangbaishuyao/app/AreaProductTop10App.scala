package com.shangbaishuyao.app

import java.util.Properties

import com.shangbaishuyao.udf.CityRatioUDAF
import com.shangbaishuyao.utils.PropertiesUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/**
 * Desc: 需求分析四: 各区域点击量Top3 商品 <br/>
 *
 * 需求分析: 按大区计算商品被点击的次数前3名，并计算出前2名占比及其他城市综合占比，结果数据展示：
 *
 * 地区	商品名称 	点击次数		 城市备注
 * 华北	商品A	    100000		 北京21.2%，天津13.2%，其他65.6%
 * 华北	商品P	    80200		   北京63.0%，太原10%，其他27.0%
 * 华北	商品M	    40000		   北京63.0%，太原10%，其他27.0%
 *
 * create by shangbaishuyao on 2021/3/11
 *
 * @Author: 上白书妖
 * @Date: 12:09 2021/3/11
 */
object AreaProductTop10App {

  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession
                                .builder()
                                .appName("AreaProductTop10App")
                                .master("local[*]")
                                .enableHiveSupport() //添加对hive的支持
                                .getOrCreate()

    //导入隐式转换
    import spark.implicits._

    //注册UDAF函数
    spark.udf.register("cityRatio",CityRatioUDAF)

    //获取用户点击行为数据
    spark.sql("select click_product_id,city_id from user_visit_action where click_product_id>0").createTempView("uv")

    //4.获取点击商品城市，大区以及商品名称
    spark.sql("select area,product_name,city_name from uv join product_info pi on uv.click_product_id=pi.product_id join city_info ci on uv.city_id=ci.city_id").createTempView("t1")

    //5.统计大区每个商品被点击的总次数
    spark.sql("select area,product_name,count(*) ct,cityRatio(city_name) city_ratio  from t1 group by area,product_name").createTempView("t2")

    //6.按照大区内商品被点击次数倒序打标签
    spark.sql("select area,product_name,ct,city_ratio,rank() over(partition by area order by ct desc) rk from t2").createTempView("t3")

    val taskId: Long = System.currentTimeMillis()

    //7.根据rank过滤点击次数前三
    val df: DataFrame = spark.sql(s"select $taskId task_id,area,product_name,ct product_count,city_ratio city_click_ratio from t3 where rk<=3")

    //读取配置
    val properties: Properties = PropertiesUtil.load("config.properties")

    //8.保存到MySQL
    df.write
      .format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info")
      .mode(SaveMode.Append)
      .save()
    //9.关闭资源
    spark.stop()

  }

}
