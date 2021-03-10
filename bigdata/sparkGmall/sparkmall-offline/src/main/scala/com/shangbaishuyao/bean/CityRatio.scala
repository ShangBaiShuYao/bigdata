package com.shangbaishuyao.bean

case class CityRatio(name: String, ratio: Double) {

  override def toString: String = s"$name$ratio%"

}
