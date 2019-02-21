package com.atguigu.spakmall.realtime.app

import com.atguigu.spakmall.realtime.bean.AdsLog
import com.atguigu.spakmall.realtime.handler.{AdsBlackListHandler, AdsClickcountHandler, AreaTop3AdsHandler}
import com.atguigu.sparkmall.common.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeApp {
  def main(args: Array[String]): Unit = {
    //1.1 搭建sparkStreaming环境
    val conf: SparkConf = new SparkConf().setAppName("sparkmall-realtime").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    // 设置checkpoint存储目录
    sc.setCheckpointDir("./checkpoint")
    
    //1.2 数据准备,从kafka读取数据并将数据转换成实时的数据流dStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)
    //ConsumerRecord[String, String]中的是kv结构,k是空的,v存的是数据,字符串
    // timestamp + " " + area + " " + city + " " + userid + " " + adid
    //第一次map是为了将ConsumerRecord[String, String]中的V的值取出来
    //第二次map是为了调整数据结构
    val adsLogDStream: DStream[AdsLog] = recordDStream.map(_.value()).map { log =>
        val adsLog: Array[String] = log.split(" ")
        AdsLog(adsLog(0).toLong, adsLog(1), adsLog(2), adsLog(3), adsLog(4))
    }

    //1.2.1 先对数据进行过滤,以下操作使用的都是过滤过的数据
    val filteredAdsLogDStream: DStream[AdsLog] = AdsBlackListHandler.check(sc,adsLogDStream)
    
    //2 需求四 : 广告黑名单实时统计 ,通过Redis对点击量进行累加
    AdsBlackListHandler.handle(filteredAdsLogDStream)
    
    //3 需求五: 广告点击量实时统计  ,通过checkpoint对点击量进行累加,结果保存到Redis中
    // 对每天 各地区 各城市 各广告 的点击流量 实时统计
    val adsClickcountDStream: DStream[(String, Long)] = AdsClickcountHandler.handle(filteredAdsLogDStream)
    
    
    //4 需求六: 实时数据分析,  每天 各地区  top3热门广告,结果存到Redis中
    AreaTop3AdsHandler.handle(adsClickcountDStream)
    
    ssc.start()
    ssc.awaitTermination()
  }
}
