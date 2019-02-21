package com.atguigu.spakmall.realtime.handler

import com.atguigu.sparkmall.common.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis


/*
    需求六: 实时数据分析,  每天 各地区  top3热门广告,结果存到Redis中
 */
object AreaTop3AdsHandler {
  //adsClickcountDStream --> (day:area:city:ads,count)
  def handle(adsClickcountDStream: DStream[(String, Long)]) = {

    //1 将(day:area:city:ads,count) 转换成(day:area:ads,count),并对count进行累加
    val areaAdsCount: DStream[(String, Long)] = adsClickcountDStream.map { case (key, count) =>
      val strings: Array[String] = key.split(":")
      val day: String = strings(0)
      val area: String = strings(1)
      val ads: String = strings(3)
      //println(count)
      (day + ":" + area + ":" + ads, count)
    }.reduceByKey(_ + _)

    //2 转化结构为(day,(area,(ads,count))),并按天进行分组
    val areaAdsCountGroupByDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCount.map { case (day_area_ads, count) =>
      val strings: Array[String] = day_area_ads.split(":")
      val day: String = strings(0)
      val area: String = strings(1)
      val ads: String = strings(2)
      //println(count)
      (day, (area, (ads, count)))
    }.groupByKey()


    //3 在day相同的情况下,对itr进行操作,按area分组 按count排序 截取top3
    val dayToAreaAndAdsCountDStream: DStream[(String, Map[String, String])] = areaAdsCountGroupByDayDStream.map { case (day, itr) =>
      
      //按area进行分组-->(area,(area,(ads,count)))  ,多余了一个area
      val areaToAdsCountGroupByAreaMap: Map[String, Iterable[(String, (String, Long))]] =
          itr.groupBy { case (area, (ads, count)) => area }

      //(area,)
      val top3AdsToJsonMap: Map[String, String] = areaToAdsCountGroupByAreaMap.map { case (area, itr) =>
          //修整结构,把itr中的area去掉,然后转成list,进行排序并截取top3
          val top3AdsToCountList: List[(String, Long)] = itr.map { case (area, (ads, count)) => (ads, count) }.toList.sortWith(_._2 > _._2).take(3)
          //将(ads,count)的list转成json字符串,因为要向Redis中存
          import org.json4s.JsonDSL._ //可以将scala中的集合隐式转换为json4s类型的值,然后转成json
          val top3AdsToJson: String = JsonMethods.compact(JsonMethods.render(top3AdsToCountList))
          //println(top3AdsToJson)
          (area, top3AdsToJson)
      }
      //最终结构(day,map(area,json(ads,count)))
      (day, top3AdsToJsonMap)
    }

    //将结果存储到Redis中
    dayToAreaAndAdsCountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { itr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        itr.foreach { case (day, areaMap) =>
          import collection.JavaConversions._ //areaMap是java中的map,这里要的是scala中的map,要转一下
          jedis.hmset("area_ads_top3:" + day, areaMap)
        }
        jedis.close()
      }
    }

  }
}
