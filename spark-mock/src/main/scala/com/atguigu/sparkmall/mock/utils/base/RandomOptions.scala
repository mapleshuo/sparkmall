package com.atguigu.sparkmall.mock.utils.base

import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomOptions {

  def apply[T](opts:RanOpt[T]*): RandomOptions[T] ={
    val randomOptions=  new RandomOptions[T]()
    for (opt <- opts ) {
      randomOptions.totalWeight+=opt.weight
      for ( i <- 1 to opt.weight ) {
        randomOptions.optsBuffer+=opt.value
      }
    }
    randomOptions
  }


  def main(args: Array[String]): Unit = {
    //zhang3和li4出现的概率理论上是5:10,也即权重
    val randomName = RandomOptions(RanOpt("zhang3",5),RanOpt("li4",10))
    for (i <- 1 to 5 ) {
      println(i+":"+randomName.getRandomOpt())

    }
  }


}

//value:数据值,weight:该值的权重(即该value的个数),例如("学生",10)
case class RanOpt[T](value:T,weight:Int){
}

class RandomOptions[T](opts:RanOpt[T]*) {
  //共有多少个
  var totalWeight=0
  var optsBuffer  =new ListBuffer[T]

  def getRandomOpt(): T ={
    //[0,totalWeight)的随机数,刚好对应集合的所有下标
    val randomNum= new Random().nextInt(totalWeight)
    //获取随机下标处的value
    optsBuffer(randomNum)
  }
}
