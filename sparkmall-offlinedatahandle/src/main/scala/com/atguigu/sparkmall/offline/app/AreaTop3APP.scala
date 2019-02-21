package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.utils.PropertiesUtil
import com.atguigu.sparkmall.offline.udf.CityRatioUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
    用SQL实现需求!!
 */
object AreaTop3APP {

  //需求三:找出各个  区域  top3热门商品(点击量top3)，并 备注 上每个商品在主要城市中的分布比例，
  // 超过两个城市用其他显示(和前两个需求没联系)
  //将结果保存到mysql中

  def main(args: Array[String]): Unit = {
    //构建spark环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("offline-areaTop3APP")
    val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //1.1 加载配置文件
    val prop: Properties = PropertiesUtil.load("config.properties")
    val url: String = prop.getProperty("jdbc.url")
    val user: String = prop.getProperty("jdbc.user")
    val password: String = prop.getProperty("jdbc.password")

    //1.2 注册自定义函数
    sparkSession.udf.register("city_ratio",new CityRatioUDAF)

    sparkSession.sql("use sparkmall2019")

    //2 关联城市表和用户行为表,将需要的数据查出来(地区,城市,商品),并转换成临时视图(相当于是给查询到的结果的虚表其了个别名)
    sparkSession.sql("select ci.area,ci.city_name,click_product_id  from user_visit_action uva join city_info ci where  uva.city_id=ci.city_id and click_product_id >0").createOrReplaceTempView("area_city_product")

    //3 按地区和商品分组找出并对click_product_id进行数量统计(area,product,click_count,city_ratio),并转成临时表
    sparkSession.sql("select area,click_product_id ,count(*) clickcount, city_ratio(city_name) city_remark from area_city_product group by  area,click_product_id").createOrReplaceTempView("area_product_clickcount_cityratio")

    //4 按地区对商品进行排名,并截取top3
    sparkSession.sql("select area,click_product_id ,clickcount,city_remark from (select apcc.*, dense_rank() over(partition by area order by clickcount desc ) rk  from area_product_clickcount_cityratio apcc) clickrk where rk<=3").createOrReplaceTempView("area_top3product_clickcount_cityratio")

    //5 将top3的商品 与商品表关联 获取到商品名
    sparkSession.sql("select area,p.product_name,clickcount,city_remark from area_top3product_clickcount_cityratio t3 ,product_info p where t3.click_product_id=p.product_id")
      .write.format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable","area_product_top3_info").mode(SaveMode.Append).save()
    sparkSession.close()

  }
}
