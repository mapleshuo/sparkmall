package com.atguigu.sparkmall.mock.utils.base

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object RandomDate {
  //测试
  def main(args: Array[String]): Unit = {
    val randomDate: RandomDate = RandomDate(new SimpleDateFormat("yyyy-MM-dd").parse("2019-02-17")
                                            ,new SimpleDateFormat("yyyy-MM-dd").parse("2019-02-18")
                                            ,10)
    //循环的次数超过给定的划分的份数时得出的随机时间会超出给出的时间范围
    for (i <- 1 to 10){

      val date: Date = randomDate.getRandomDate()
      println(s"date:$date")
    }
  }

  def apply(startDate:Date,endDate:Date,step:Int): RandomDate ={
    val randomDate = new RandomDate()
    val avgStepTime = (endDate.getTime- startDate.getTime)/step
    randomDate.maxTimeStep=avgStepTime*2
    randomDate.lastDateTime=startDate.getTime
    randomDate
  }


  class RandomDate{
    var lastDateTime =0L
    var maxTimeStep=0L

    //获取随机时间
    def  getRandomDate()={
      //获取随机步长
      val timeStep = new Random().nextInt(maxTimeStep.toInt)
      //通过上次的时间加上随机步长来得到随机时间
      lastDateTime = lastDateTime+timeStep
      //将随机时间封装成Date
      new Date( lastDateTime)
    }
  }
}
