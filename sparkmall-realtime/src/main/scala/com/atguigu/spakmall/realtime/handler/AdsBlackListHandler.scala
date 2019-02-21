package com.atguigu.spakmall.realtime.handler

import java.util

import com.atguigu.spakmall.realtime.bean.AdsLog
import com.atguigu.sparkmall.common.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/*
    需求四:广告黑名单实时统计
 */
object AdsBlackListHandler {

  def handle(adsLogDStream: DStream[AdsLog]) = {


    //1 调整数据结构,统计出每日 每用户 点击的某广告的次数
    val adsCountDStream: DStream[(String, Long)] = adsLogDStream.map { adsLog =>
      //每日 每用户 点击的广告  ,点击广告的次数
      (adsLog.getDate + "_" + adsLog.userId + "_" + adsLog.adsId, 1L)
    }.reduceByKey(_ + _)

    //2 将数据写入到Redis中(Hash)
    adsCountDStream.foreachRDD { rdd =>
      //对每个rdd进行操作
      rdd.foreachPartition { case adsItr =>
        //获取Redis连接对象
        val jedis: Jedis = RedisUtil.getJedisClient
        adsItr.foreach { case (ads, count) =>
          //将ads切分
          val day_user_ad: Array[String] = ads.split("_")
          //将需要的数据取出来
          val day: String = day_user_ad(0)
          val user: String = day_user_ad(1)
          val ad: String = day_user_ad(2)
          val key: String = "user_click_ad" + ":" + day

          //将数据存到Redis,当field相同时会对count进行累加
          jedis.hincrBy(key, user + "_" + ad, count)
          //添加之后对count(点击次数进行判断,当count>100时将用户添加到黑名单)  
          val curCount: Long = jedis.hget(key, user + "_" + ad).toLong
          if (curCount >= 100) {
            //黑名单用set,可去重
            jedis.sadd("blacklist", user)
          }
        }
        jedis.close()
      }


    }
  }

  //对黑名单的用户信息进行过滤
  def check(sc: SparkContext,adsLogDStream: DStream[AdsLog]) = {
    
    val filteredAdsLogDStream: DStream[AdsLog] = adsLogDStream.transform { rdd =>
      //下面这三行代码会在driver中执行,每个rdd执行一侧
      //创建Redis连接
      val jedis: Jedis = RedisUtil.getJedisClient
      //获取黑名单数据
      val blacklistSet: util.Set[String] = jedis.smembers("blacklist")
      //将黑名单数据放到广播变量中
      val blacklistSetBC: Broadcast[util.Set[String]] = sc.broadcast(blacklistSet)

      //过滤
      rdd.filter { ads =>
        !blacklistSetBC.value.contains(ads.userId)
      }
    }
    filteredAdsLogDStream
  }


}
