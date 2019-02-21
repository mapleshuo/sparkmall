package com.atguigu.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

/*
    自定义UDAF函数,作用于某一个字段
 */
class CityRatioUDAF extends UserDefinedAggregateFunction {
  //1 定义输入数据的结构类型,输入的是String类型的数据
  override def inputSchema: StructType = {
    //StructType是一个大的结构,里面是一个数组,数组中是一个个的类型(这里只写了一个类型)
    StructType(Array(StructField("city_name", StringType)))
  }

  //2 定义存储结构类型,这个存储结构是为了方便处理传入数据的
  override def bufferSchema: StructType = {
    //同上,这里有两种类型,一,map类型,二,long类型
    StructType(Array(StructField("city_count", MapType(StringType, LongType)), StructField("total_count", LongType)))
  }

  //3 定义输出数据的类型
  override def dataType: DataType = {
    StringType
  }

  //4 当出入参数相同时输出结果是否也相同(一般当函数的数据有时间或随机数等数据时输入相同参数时输出结果才会不一样)
  override def deterministic: Boolean = {
    true
  }

  //5 初始化存储结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //定义存储结构时,定义了几个就有几个参数,这里是两个
    //第一个是map类型的
    buffer(0) = new HashMap[String, Long]()
    //第二个是long类型的
    buffer(1) = 0L
  }

  //6 当有数据传入时,更新存储中的数据(可以看做是RDD的一个分区内的每条数据)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //先将传入的数据列出来
    //存储中的数据
    val cityAndCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    //输入的数据
    val cityname: String = input.getString(0)

    //将输入的数据添加到存储中
    buffer(0) = cityAndCountMap + (cityname -> (cityAndCountMap.getOrElse(cityname, 0L) + 1L))
    //因为每次都是一条数据所以加一就行
    buffer(1) = totalCount + 1L

  }

  //7 合并数据(看做是对各个分区间的数据做合并)
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //这里的buffer1是这里的存储,buffer2是其他另一个分区的存储

    //先将两个存储中的数据都取出来
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    //通过foldLeft进行合并,并将合并后的数据更新到存储中
    buffer1(0) = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap2, (city, count)) =>
      cityCountMap2 + (city -> (cityCountMap2.getOrElse(city, 0L) + count))
    }

    buffer1(1) = totalCount1 + totalCount2

  }

  //8 计算出最终需要的结果(字符串)
  override def evaluate(buffer: Row): Any = {
    //将最终合并后的数据取出来
    val cityToCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalcount: Long = buffer.getLong(1)

    //转换结构(city,cityRatio),计算百分比,再将数据转成list,方便排序
    val cityToCityRatioList: List[CityAndCityRatio] = cityToCount.map { case (city, count) =>
      //乘1000是为了保留一位小数,math.round()是四舍五入取整数,这样得到的是个三位的整数,再除10D就变成xx.x的小数了
      val cityRatio: Double = math.round(count.toDouble / totalcount * 1000) / 10D
      CityAndCityRatio(city, cityRatio)
    }.toList


    //排序并截取top2
    var cityTop2List: List[CityAndCityRatio] = cityToCityRatioList.sortBy(_.cityRatio)(Ordering.Double.reverse).take(2)

    //计算"其他城市"的百分比
    if (cityToCityRatioList.size > 2) {
      //拿100去减集合中的两个的百分比就得出了"其他城市"的百分比
      var otherRatio = 100D
      //这时取到了"其他城市"的百分比,但是该值后面有很多位小数
      cityTop2List.foreach(item => {
        otherRatio -= item.cityRatio
      })

      //保留一位小数
      otherRatio = math.round(otherRatio * 10) / 10D

      //将("其他城市",百分比)添加到集合
      cityTop2List = cityTop2List :+ CityAndCityRatio("其他", otherRatio)

    }

    //将集合转换 成字符串
    cityTop2List.mkString(",")
  }


  case class CityAndCityRatio(cityName: String, cityRatio: Double) {
    //重写tostring方法(cityName:cityRatio%)
    override def toString: String = {
      cityName + ":" + cityRatio + "%"
    }
  }

}
