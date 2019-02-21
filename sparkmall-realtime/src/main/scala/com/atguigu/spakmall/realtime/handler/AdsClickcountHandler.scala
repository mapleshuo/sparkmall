package com.atguigu.spakmall.realtime.handler

import com.atguigu.spakmall.realtime.bean.AdsLog
import com.atguigu.sparkmall.common.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/*
    需求五: 广告点击量实时统计
    每天各地区各城市各广告的点击流量实时统计
 */
object AdsClickcountHandler {
  
    def handle(adsLogDStream: DStream[AdsLog]) ={
      //1 先转换结构(day:area:city:adsid,1L)
      val clickCountDStream: DStream[(String, Long)] = adsLogDStream.map { item =>
        (item.getDate + ":" + item.area + ":" + item.city + ":" + item.adsId, 1L)
      }
      
      //2 通过updataStageByKey得到想要的结果,(day:area:city:adsid,sum)
      val clickCountSumDStream: DStream[(String, Long)] = clickCountDStream.updateStateByKey { (count: Seq[Long], curcount: Option[Long]) =>
        //第一个参数是相同key对应的value,第二个参数是checkpoint中当前存储的值
        //我们要做的是将value累加起来然后和checkpoint中的值相加,对checkpoint的值进行更新
        //对count进行累加
        val countSum: Long = count.sum
        //让countSum加上checkpoint的值
        val newCount: Long = curcount.getOrElse(0L) + countSum
        //println(newCount+",")
        //更新checkpoint
        Some(newCount)
      }
      //将结果存储到Redis中
      clickCountSumDStream.foreachRDD{rdd=>
        rdd.foreachPartition{itr=>
          
          //每个分区获取一个Redis连接
          val jedis: Jedis = RedisUtil.getJedisClient
          
          //将数据存储到Redis中
          itr.foreach{case (key,count)=>
            jedis.hset("day_area_city_ad_count",key,count.toString)
          }
          
          jedis.close()
        }
      }
      
      //将结果返回
      clickCountSumDStream
    }
}
