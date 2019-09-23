package com.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 10-38
  * 测试工具类：key：0802edbff99345213340e64730f701c0
  */
object Test {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("TagTest").master("local[*]").getOrCreate()
    val arr = Array("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=0802edbff99345213340e64730f701c0&radius=1000&extensions=all")
    val rdd: RDD[String] = spark.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HttpUtil.get(t)
    }).foreach(println)
  }
}
