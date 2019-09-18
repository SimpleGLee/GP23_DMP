package com.Location

import com.util.ReqUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 09-32
  * 地域分布-sparkCode
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    //判断是否有输入路径
    if(args.length != 2){
      println("目录路径不正确")
      sys.exit()
    }
    //指定输入路径参数
    val Array(inputPath,outputPath) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDist")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //获取字段
    val df: DataFrame = spark.read.parquet(inputPath)
    //
    df.rdd.map(row=>{
      // 根据指标的字段获取数据
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      //处理请求数
      val reqList: List[Double] = ReqUtils.ReqAreaProCity(requestmode,processnode)
      //处理展示点击数
      val clickList: List[Double] = ReqUtils.ClickAreaProCity(requestmode,iseffective)
      //处理广告
      val adList: List[Double] = ReqUtils.AdSuccAreaProCity(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //将集合相加
      val allList: List[Double] = reqList ++ clickList ++ adList

      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      //将两个集合做拉链操作
      //list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))//指定输出格式
      .saveAsTextFile(outputPath)


  }
}
