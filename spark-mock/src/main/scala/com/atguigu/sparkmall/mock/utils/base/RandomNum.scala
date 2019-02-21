package com.atguigu.sparkmall.mock.utils.base

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {
  //测试
  def main(args: Array[String]): Unit = {
    println(multi(1, 19, 5, "|", false))
  }

  //在指定范围内获取一个随机数
  def apply(fromNum:Int,toNum:Int): Int =  {
    //获取[fromNum,toNum]内的随机数
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }

  //在指定范围[fromNum,toNum]内,获取amount个随机数,用指定分隔符隔开delimiter,
  // canRepeat(指定获取的随机数能不能重复)
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复

    if (canRepeat) {
      //创建一个可变的list集合
      val numList = new ListBuffer[Int]()
      //向集合中添加amount个随机数
      while (numList.size<amount) {
        numList+=apply(fromNum,toNum)
      }
      //将集合转换成string返回
      numList.mkString(delimiter)

    }else {
      val numSet: mutable.HashSet[Int] = new mutable.HashSet[Int]()
      while (numSet.size<amount) {
        numSet+=apply(fromNum,toNum)
      }
      numSet.mkString(delimiter)
    }
  }


}
