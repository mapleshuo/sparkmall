package com.atguigu.sparkmall.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.utils.PropertiesUtil
import com.atguigu.sparkmall.offline.bean.CategoryCount
import com.atguigu.sparkmall.offline.handler.{CategoryTop10Handler, SessionTop10Handler}
import com.atguigu.sparkmall.offline.utils.ReadHivedatatoRDD
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/*
    需求一,二
 */
object OfflineApp {
  def main(args: Array[String]): Unit = {

    //生成taskId
    val taskId: String = UUID.randomUUID().toString

    //构建sparksql环境
    val conf: SparkConf = new SparkConf().setAppName("spark-offline").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //根据条件从hive中查询并获取数据
    val userVisitActionRDD: RDD[UserVisitAction] = ReadHivedatatoRDD.readHivedataToRDD(sparkSession)

    //测试数据是否成功获取
    //userVisitActionRDD.foreach(println(_))

    //需求一:获取点击、下单和支付数量排名前 10 的品类
    val top10CidList: List[CategoryCount] = CategoryTop10Handler.handle(sparkSession,userVisitActionRDD,taskId)
    println("需求一结束!")

    //需求二:分别获取top10品类点击次数排名前 10 的 sessionId。
    SessionTop10Handler.handler(taskId,sparkSession,userVisitActionRDD,top10CidList)
    println("需求二结束!")

  }


}
