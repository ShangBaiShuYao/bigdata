package com.shangbaishuyao

import java.util

import com.alibaba.fastjson.JSON
import org.json4s.jackson.JsonMethods

object Test {

  def main(args: Array[String]): Unit = {

    val list = List(("a", 1), ("b", 2))
    import org.json4s.JsonDSL._
    println(JsonMethods.compact(list))

    val arrayList = new util.ArrayList[(String, Int)]()
    arrayList.add(("a", 1))
    arrayList.add(("b", 2))
    println(JSON.toJSON(arrayList))

  }

}
