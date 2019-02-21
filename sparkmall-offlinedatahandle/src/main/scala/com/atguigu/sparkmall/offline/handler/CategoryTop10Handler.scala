package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.utils.JdbcUtil
import com.atguigu.sparkmall.offline.acc.CategoryAccumulator
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

//需求一:获取点击、下单和支付数量排名前 10 的品类
object CategoryTop10Handler {


  def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
    //1.创建自定义累加器对象,注册累加器
    val accumulator: CategoryAccumulator = new CategoryAccumulator()
    sparkSession.sparkContext.register(accumulator)

    //2.对userVisitActionRDD进行遍历,将所需的数据从RDD中拆解出来,并用累加器进行累加,最终获得(cid_action,count)
    userVisitActionRDD.foreach(itme => {
      if (itme.click_category_id != -1L) {

        val key: String = itme.click_category_id + "_click"
        accumulator.add(key)

      } else if (itme.order_category_ids != null && itme.order_category_ids.size > 0) {

        val categoryIds: Array[String] = itme.order_category_ids.split(",")
        for (cid <- categoryIds) {
          val key: String = cid + "_order"
          accumulator.add(key)
        }

      } else if (itme.pay_category_ids != null && itme.pay_category_ids.size > 0) {

        val categoryIds: Array[String] = itme.pay_category_ids.split(",")
        for (cid <- categoryIds) {
          val key: String = cid + "_pay"
          accumulator.add(key)
        }

      }
    })

    //获取累加器中的数据(cid_action,count)
    val cidToCountMap: mutable.HashMap[String, Long] = accumulator.value
    //println(cidToCountMap.mkString("\n"))

    //3.通过品类id将数据进行分组(cid,seq(hashmap(cid_action,count)...))
    val groupbyCidMap: Map[String, mutable.HashMap[String, Long]] = cidToCountMap.groupBy({ case (key, count) =>
      key.split("_")(0)
    })
    //4.将数据进行转换先封装进对象中再放到集合中(放到集合中方便进行排序)
    val categoryCountList: List[CategoryCount] = groupbyCidMap.map { case (cid, cidmap) =>
      CategoryCount(taskId
                    , cid
                    , cidmap.getOrElse(cid + "_click", 0L)
                    , cidmap.getOrElse(cid + "_order", 0L)
                    , cidmap.getOrElse(cid + "_pay", 0L)
                  )
    }.toList

    //5.对集合中的数据进行排序,并截取top10
    val top10CidList: List[CategoryCount] = categoryCountList.sortWith((categoryCount1, categoryCount2) =>
      //先比较点击量
      if (categoryCount1.clickCount > categoryCount2.clickCount) {
        true
      } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
            if(categoryCount1.orderCount>categoryCount2.orderCount){
                true
            }else if(categoryCount1.orderCount==categoryCount2.orderCount){
                if(categoryCount1.payCount>categoryCount2.payCount){
                  true
                }else{
                  false
                }

            }else{
              false
            }
      }else{
        false
      }
    ).take(10)
    //println(top10Cid.mkString("\n"))

    //6.先将数据转换成数组形式,然后保存到mysql中
    val resultList: List[Array[Any]] = top10CidList.map(item => {
      Array(taskId, item.categoryId, item.clickCount, item.orderCount, item.payCount)
    }
    )

  JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",resultList)

    top10CidList
  }
}
