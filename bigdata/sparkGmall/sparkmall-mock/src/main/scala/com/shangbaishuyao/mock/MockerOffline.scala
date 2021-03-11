package com.shangbaishuyao.mock

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.shangbaishuyao.datamode.{CityInfo, ProductInfo, UserInfo, UserVisitAction}
import com.shangbaishuyao.utils.{RanOpt, RandomDate, RandomNum, RandomOptions}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}

import scala.collection.mutable.ListBuffer
/**
 * Desc:  生成离线数据 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 14:36 2021/3/9
 */
object MockerOffline {

  val userNum: Int = 100
  val productNum: Int = 100
  val sessionNum: Int = 10000

  val pageNum: Int = 50
  val cargoryNum: Int = 20

  val logAboutNum: Int = 100000 //日志大致数量，用于分布时间

  val professionRandomOpt: RandomOptions[String] = RandomOptions(RanOpt("学生", 40), RanOpt("程序员", 30), RanOpt("经理", 20), RanOpt("老师", 10))

  val genderRandomOpt: RandomOptions[String] = RandomOptions(RanOpt("男", 60), RanOpt("女", 40))
  val ageFrom: Int = 10
  val ageTo: Int = 59

  val productExRandomOpt: RandomOptions[String] = RandomOptions(RanOpt("自营", 70), RanOpt("第三方", 30))

  val searchKeywordsOptions: RandomOptions[String] = RandomOptions(RanOpt("手机", 30), RanOpt("笔记本", 70), RanOpt("内存", 70), RanOpt("i7", 70), RanOpt("苹果", 70), RanOpt("吃鸡", 70))

  val actionsOptions: RandomOptions[String] = RandomOptions(RanOpt("search", 20), RanOpt("click", 60), RanOpt("order", 6), RanOpt("pay", 4), RanOpt("quit", 10))


  /**
   * 运行则生成离线数据 <br/>
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Mock").setMaster("local[*]")
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 模拟数据
    val userVisitActionData: List[UserVisitAction] = this.mockUserAction()
    val userInfoData: List[UserInfo] = this.mockUserInfo()
    val productInfoData: List[ProductInfo] = this.mockProductInfo()
    val cityInfoData: List[CityInfo] = this.mockCityInfo()

    // 将模拟数据装换为RDD
    val userVisitActionRdd: RDD[UserVisitAction] = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd: RDD[UserInfo] = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRdd: RDD[ProductInfo] = spark.sparkContext.makeRDD(productInfoData)
    val cityInfoRdd: RDD[CityInfo] = spark.sparkContext.makeRDD(cityInfoData)

    val userVisitActionDF: DataFrame = userVisitActionRdd.toDF()
    val userInfoDF: DataFrame = userInfoRdd.toDF()
    val productInfoDF: DataFrame = productInfoRdd.toDF()
    val cityInfoDF: DataFrame = cityInfoRdd.toDF()

    insertHive(spark, "user_visit_action", userVisitActionDF)
    insertHive(spark, "user_info", userInfoDF)
    insertHive(spark, "product_info", productInfoDF)
    insertHive(spark, "city_info", cityInfoDF)

    spark.close()
  }

  def insertHive(sparkSession: SparkSession, tableName: String, dataFrame: DataFrame): Unit = {
    sparkSession.sql("drop table if exists " + tableName)
    dataFrame.write.saveAsTable(tableName)
    println("保存：" + tableName + "完成")
    sparkSession.sql("select * from " + tableName).show(100)

  }

  def mockUserInfo(): List[UserInfo] = {

    val rows = new ListBuffer[UserInfo]()

    for (i <- 1 to userNum) {
      val user = UserInfo(i,
        "user_" + i,
        "name_" + i,
        RandomNum(ageFrom, ageTo), //年龄
        professionRandomOpt.getRandomOpt,
        genderRandomOpt.getRandomOpt
      )
      rows += user
    }
    rows.toList
  }

  def mockUserAction(): List[UserVisitAction] = {

    val rows = new ListBuffer[UserVisitAction]()

    val startDate: Date = new SimpleDateFormat("yyyy-MM-dd").parse("2019-11-26")
    val endDate: Date = new SimpleDateFormat("yyyy-MM-dd").parse("2019-11-27")
    val randomDate = RandomDate(startDate, endDate, logAboutNum)
    for (i <- 1 to sessionNum) {
      val userId = RandomNum(1, userNum)
      val sessionId: String = UUID.randomUUID().toString
      var isQuit = false

      while (!isQuit) {
        val action: String = actionsOptions.getRandomOpt

        if (action.equals("quit")) {
          isQuit = true
        } else {
          val actionDateTime: Date = randomDate.getRandomDate
          val actionDateString: String = new SimpleDateFormat("yyyy-MM-dd").format(actionDateTime)
          val actionDateTimeString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(actionDateTime)

          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null

          var cityId: Long = RandomNum(1, 26).toLong

          action match {
            case "search" => searchKeyword = searchKeywordsOptions.getRandomOpt
            case "click" => clickCategoryId = RandomNum(1, cargoryNum)
              clickProductId = RandomNum(1, productNum)
            case "order" => orderCategoryIds = RandomNum.multi(1, cargoryNum, RandomNum(1, 5), ",", canRepeat = false)
              orderProductIds = RandomNum.multi(1, cargoryNum, RandomNum(1, 5), ",", canRepeat = false)
            case "pay" => payCategoryIds = RandomNum.multi(1, cargoryNum, RandomNum(1, 5), ",", canRepeat = false)
              payProductIds = RandomNum.multi(1, cargoryNum, RandomNum(1, 5), ",", canRepeat = false)
          }

          val userVisitAction = UserVisitAction(
            actionDateString,
            userId.toLong,
            sessionId,
            RandomNum(1, pageNum).toLong,
            actionDateTimeString,
            searchKeyword,
            clickCategoryId,
            clickProductId,
            orderCategoryIds,
            orderProductIds,
            payCategoryIds,
            payProductIds,
            cityId
          )
          rows += userVisitAction
        }
      }

    }
    rows.toList
  }

  def mockProductInfo(): List[ProductInfo] = {
    val rows = new ListBuffer[ProductInfo]()
    for (i <- 1 to productNum) {
      val productInfo = ProductInfo(
        i,
        "商品_" + i,
        productExRandomOpt.getRandomOpt
      )
      rows += productInfo
    }
    rows.toList
  }

  def mockCityInfo(): List[CityInfo] = {
    List(CityInfo(1L, "北京", "华北"), CityInfo(2L, "上海", "华东"),
      CityInfo(3L, "深圳", "华南"), CityInfo(4L, "广州", "华南"),
      CityInfo(5L, "武汉", "华中"), CityInfo(6L, "南京", "华东"),
      CityInfo(7L, "天津", "华北"), CityInfo(8L, "成都", "西南"),
      CityInfo(9L, "哈尔滨", "东北"), CityInfo(10L, "大连", "东北"),
      CityInfo(11L, "沈阳", "东北"), CityInfo(12L, "西安", "西北"),
      CityInfo(13L, "长沙", "华中"), CityInfo(14L, "重庆", "西南"),
      CityInfo(15L, "济南", "华东"), CityInfo(16L, "石家庄", "华北"),
      CityInfo(17L, "银川", "西北"), CityInfo(18L, "杭州", "华东"),
      CityInfo(19L, "保定", "华北"), CityInfo(20L, "福州", "华南"),
      CityInfo(21L, "贵阳", "西南"), CityInfo(22L, "青岛", "华东"),
      CityInfo(23L, "苏州", "华东"), CityInfo(24L, "郑州", "华北"),
      CityInfo(25L, "无锡", "华东"), CityInfo(26L, "厦门", "华南")
    )
  }
}