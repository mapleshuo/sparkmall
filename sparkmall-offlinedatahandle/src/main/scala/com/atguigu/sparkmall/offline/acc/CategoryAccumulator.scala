package com.atguigu.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
/*
自定义累加器
 */
class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  //创建可变集合
  var categoryMap: mutable.HashMap[String, Long] = new mutable.HashMap[String,Long]()


  override def isZero: Boolean = {
    categoryMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator()
  }

  override def reset(): Unit = {
    categoryMap.clear()
  }
  //分区内累加
  override def add(key: String): Unit = {

    categoryMap(key)=categoryMap.getOrElse(key,0L) + 1L
  }
  //分区间累加
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //先获取要合并的分区的值
    val otherMap: mutable.HashMap[String, Long] = other.value

    //合并两个分区的值
    categoryMap = categoryMap.foldLeft(otherMap){case (otherMap,(key,count))=>{
      otherMap(key) = otherMap.getOrElse(key,0L) + count
      otherMap
    }}
  }

  override def value: mutable.HashMap[String, Long] = {
    categoryMap
  }
}
