package com.SparkProjectTest1

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * Write By SimpleLee 
  * On 2019-九月-星期日
  * 10-55
  */
object exam01 {
  def main(args: Array[String]): Unit = {
    //设定目录限制
    if(args.length != 1){
      println("目录不正确，退出程序")
      sys.exit()
    }

    //获取目录参数
    var Array(inputPath) = args
    val sparkSession: SparkSession = SparkSession.builder().appName("exam").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val dataFrame: DataFrame = sparkSession.read.json(inputPath)
//    dataFrame.rdd.map(t=>{
//      val value: JSONObject = JSON.parseObject(t)
//    })

    dataFrame.createTempView("table_json")
    //1.
    if(dataFrame.select("status")==0){
      val frame: DataFrame = dataFrame.select("regeocode")
      val str: RDD[String] = frame.map(t => {
        val jSONObject: JSONObject = JSON.parseObject(t.toString())
        val jSONObject1: JSONObject = jSONObject.getJSONObject("pois")
        val jSONObject2: JSONObject = jSONObject1.getJSONObject("businessarea")
        jSONObject2.toString()
      }).rdd

      val rdd: RDD[(String, Int)] = str.map(m => {
        (m, 1)
      })
      val reduced: RDD[(String, Int)] = rdd.reduceByKey(_+_)
      //输出结果
      println(reduced.collect().toString)
    }

    //2.
    if(dataFrame.select("status")==0){
      val frame: DataFrame = dataFrame.select("regeocode")
      val str: RDD[String] = frame.map(t => {
        val jSONObject: JSONObject = JSON.parseObject(t.toString())
        val jSONObject1: JSONObject = jSONObject.getJSONObject("pois")
        val jSONObject2: JSONObject = jSONObject1.getJSONObject("type")
        jSONObject2.toString()
      }).rdd

      val rdd: RDD[(String, Int)] = str.map(m => {
        (m, 1)
      })
      val reduced: RDD[(String, Int)] = rdd.reduceByKey(_+_)
      //输出结果
      println(reduced.collect().toString)
    }

  }

}
