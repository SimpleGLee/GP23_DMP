package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 16-45
  */
object ChannelReport {
  def main(args: Array[String]): Unit = {
    //adplatformproviderid
    //判断是否有输入路径
    if(args.length != 1){
      println("目录路径不正确")
      sys.exit()
    }
    //指定输入参数
    val Array(inputPath) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDist")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet(inputPath)
    //创建临时表
    df.createTempView("log")
    val frame: DataFrame = spark.sql("select distinct(adplatformproviderid) from log")
    frame.show()
  }
}
