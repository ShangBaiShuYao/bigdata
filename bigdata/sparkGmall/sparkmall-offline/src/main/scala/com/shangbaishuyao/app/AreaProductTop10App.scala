package com.shangbaishuyao.app

import java.util.Properties

import com.shangbaishuyao.udf.CityRatioUDAF
import com.shangbaishuyao.utils.PropertiesUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaProductTop10App {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AreaProductTop10App")
      .enableHiveSupport()
      .getOrCreate()

    //2.导入隐式转换
    import spark.implicits._

    //注册UDAF函数
    spark.udf.register("cityRatio", CityRatioUDAF)

    //3.获取用户点击行为数据
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
