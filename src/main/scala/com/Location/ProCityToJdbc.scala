package com.Location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 15-54
  */
object ProCityToJdbc {
  def main(args: Array[String]): Unit = {
    //判断是否有输入路径
    if(args.length != 1){
      println("目录路径不正确")
      sys.exit()
    }
    //指定输入路径参数
    val Array(inputPath) = args
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("ProCityCount")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = sparkSession.read.parquet(inputPath)
    //注册临时表
    df.createTempView("log")
    val frame: DataFrame = sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //通过config配置文件依赖进行加载相关的配置信息
    val config: Config = ConfigFactory.load()
    //创建properties对象
    val prop = new Properties()
    prop.setProperty("user",config.getString("jdbc.user"))
    prop.setProperty("password",config.getString("jdbc.pwd"))

    //存储
    frame.write.mode(SaveMode.Append)
      .jdbc(config.getString("jdbc.url"),config.getString("jdbc.tableName"),prop)

    sparkSession.stop()

  }
}
