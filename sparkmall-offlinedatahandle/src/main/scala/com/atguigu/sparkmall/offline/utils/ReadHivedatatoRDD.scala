package com.atguigu.sparkmall.offline.utils

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.utils.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadHivedatatoRDD {


  /***
    *   从hive中获取符合条件 的数据
    * @param sparkSession
    */
  def readHivedataToRDD(sparkSession: SparkSession ) = {
    //通过common工具类读取配置文件
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    //这里返回的是json串
    val conditionsjson: String = properties.getProperty("condition.params.json")
    //将json串解析成对象,方便取数据
    val jSONObject: JSONObject = JSON.parseObject(conditionsjson)
    //取数据
    val startDate: String = jSONObject.getString("startDate")
    val endDate: String = jSONObject.getString("endDate")
    val startAge: String = jSONObject.getString("startAge")
    val endAge: String = jSONObject.getString("endAge")

    //写SQL语句
    val sql: StringBuilder = new StringBuilder("select v.* from user_visit_action v,user_info u where v.user_id=u.user_id ")

    if (startDate.nonEmpty) {
      sql.append("and date >= '"+ startDate +"' ")
    }

    if (endDate.nonEmpty) {
      sql.append("and date <= '"+ endDate +"' ")
    }

    if (startAge.nonEmpty) {
      sql.append("and age >= '"+ startAge +"' ")
    }

    if (endAge.nonEmpty) {
      sql.append("and age >= '"+ endAge +"' ")
    }

    println(sql)

    sparkSession.sql("use sparkmall2019")
    import sparkSession.implicits._
    //执行SQL语句并将查询结果保存成RDD
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

    rdd
  }
}
