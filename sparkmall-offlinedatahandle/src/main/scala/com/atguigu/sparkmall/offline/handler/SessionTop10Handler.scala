package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.utils.JdbcUtil
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


//需求二:分别获取 top10品类 点击次数 排名前10  的sessionId
object SessionTop10Handler {
  /*
  taskId:批次id
  sparkSession
  userVisitActionRDD:所有的数据
  top10CidList:top10的CategoryCount的list
   */
  def handler(taskId: String, sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], top10CidList: List[CategoryCount]) = {
    //1.获取top10 的cid并封装成广播变量,方便在executor中使用
    val top10Cids: List[Long] = top10CidList.map(_.categoryId.toLong)
    val top10CidsBC: Broadcast[List[Long]] = sparkSession.sparkContext.broadcast(top10Cids)

    //2.将点击过top10cid的用户访问行为的数据过滤出来
    val top10RDD: RDD[UserVisitAction] = userVisitActionRDD.filter { item =>
      top10CidsBC.value.contains(item.click_category_id)
    }
    

    //3.将数据结构转化成我们需要的结构类型(cid_sessionid,1L),然后聚合成(cid_sessionid,count)
    val cidSessionidCountRDD: RDD[(String, Long)] = top10RDD.map { item =>
      (item.click_category_id + "_" + item.session_id, 1L)
    }.reduceByKey(_ + _)


    //4.因为要获取top10cid中每个cid的top10的sid,所以要以cid为条件进行分组并进行组内排序截取top10的数据

    //4.1先调整数据结构,然后分组
    val groupByedCidToSessionidCountRDD: RDD[(String, Iterable[(String, Long)])] = cidSessionidCountRDD.map { case (cid_sessionid, count) =>
      val cidAndSessionid: Array[String] = cid_sessionid.split("_")
      (cidAndSessionid(0), (cidAndSessionid(1), count)) //(cid,(sid,count))
    }.groupByKey()

    //4.2 先将这10个组的数据做扁平化处理,变成100条数据,组内排序并截取组内top10的数据
    val value: RDD[Array[Any]] = groupByedCidToSessionidCountRDD.flatMap { case (cid, sessionCountItrs) =>
      //排序并截取前十的数据
      val top10SidCount: List[(String, Long)] = sessionCountItrs.toList.sortWith { (sidCount1, sidCount2) =>
        sidCount1._2 > sidCount2._2
      }.take(10)

      //将数据转换成与mysql表对应的结构
      val list: List[Array[Any]] = top10SidCount.map { case (sid, count) =>
        Array(taskId, cid, sid, count)
      }
      list
    }

    //4.3通过行动算子使计算开始并将数据收集到driver
    val resultArray: Array[Array[Any]] = value.collect()


    //5.将数据保存到mysql中
    JdbcUtil.executeBatchUpdate("insert into top10_cid_top10_sid_count values(?,?,?,?)",resultArray)
  }
}
