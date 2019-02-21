package com.atguigu.sparkmall.common.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    //测试load方法
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
