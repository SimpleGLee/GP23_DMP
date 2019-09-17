package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 15-27
  * 将统计的省市信息以json输出（分区）
  */
object ProCityToJson {
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
    //创建临时表
    df.createTempView("log")
    val frame: DataFrame = sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    frame.write.partitionBy("provincename","cityname").json("E:\\project\\json")

  }
}
