package com.atguigu.sparkmall.common.test

import scala.collection.mutable
import scala.collection.mutable.HashMap


object MyTest {
  def main(args: Array[String]): Unit = {
    val map1: HashMap[String, Int] = HashMap(("a", 1), ("b", 1))
    val map2: HashMap[String, Int] = HashMap(("c", 1), ("d", 1))
    //println(map1 += ("g" -> 1))

    //List()
    val aa = math.round(111.11.toDouble / 1000 * 1000) / 10D
    //println(aa)
    var bb = 100D
    var cc: List[Double] = List(10.21111, 20.5111111)
    cc.foreach(t => (bb -= t))

    println(bb)
    bb = math.round(bb * 10) / 10D
    println(bb)
    cc = cc :+ bb
    println(cc)

  }
}
