package com.atguigu.spakmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class AdsLog(timestamp:Long
                  ,area:String
                  ,city:String
                  ,userId:String
                  ,adsId:String
                 ) {
  //创建日期格式化器
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //将时间戳转换成 年月日
  def getDate ={
    dateFormat.format(new Date(timestamp))
  }

}
