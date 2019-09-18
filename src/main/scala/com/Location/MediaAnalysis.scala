package com.Location

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import com.util.ReqUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 21-42
  * 媒体分析
  */
object MediaAnalysis {
  def main(args: Array[String]): Unit = {
    //判断是否有输入路径
    if(args.length != 3){
      println("目录路径不正确")
      sys.exit()
    }
    //指定输入参数
    val Array(inputPath,outputPath,docs) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDist")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据字典
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile(docs).map(_.split("\\s", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    //进行广播
    val broadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)

    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      //获取没提相关字段
      var appname: String = row.getAs[String]("appname")
      if(StringUtils.isBlank(appname)){
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = ReqUtils.ReqAreaProCity(requestmode,processnode)
      // 处理展示点击
      val clickList = ReqUtils.ClickAreaProCity(requestmode,iseffective)
      // 处理广告
      val adList = ReqUtils.AdSuccAreaProCity(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      (appname,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
        .saveAsTextFile(outputPath)

  }
}
