package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import com.util.TagsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 09-49
  * 上下文的标签主类
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs,stopwords) = args

    //创建Spark上下文
    val spark: SparkSession = SparkSession.builder().appName("TagContext").master("local[2]").getOrCreate()

    /**
      * 调用HbaseAPI
      */
    val load: Config = ConfigFactory.load()
    //获取表名
    val HbaseTableName: String = load.getString("HBASE.tableName")
    //创建Hadoop任务
    val configuration: Configuration = spark.sparkContext.hadoopConfiguration


    import spark.implicits._

    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)

    //读取字典文件--为了给APP名称打标签
    val docsRDD: collection.Map[String, String] = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5).map(arr => {
      ((arr(4), arr(1)))
    }).collectAsMap()
    //广播字典
    val broadvalue: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docsRDD)


    //读取停用字典文件--为了给APP名称打标签
    val stopwordsRDD: collection.Map[String, Int] = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()
    //广播字典
    val broadvalues: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopwordsRDD)


    //处理数据信息
    df.map(row => {
      //获取用户的唯一ID
      val userId: String = TagsUtils.GetOneUserId(row)
      //接下来标签 实现
      val adList: List[(String, Int)] = TagsAd.makeTags(row)
      //商圈
      val businessList: List[(String, Int)] = BusinessTag.makeTags(row)

      //媒体标签
      val appList: List[(String, Int)] = TagsAPP.makeTags(row,broadvalue)

      //设备标签
      val devList: List[(String, Int)] = TagsDevice.makeTags(row)

      //地域标签
      val locList: List[(String, Int)] = TagsLocation.makeTags(row)

      //关键字标签
      val kwList: List[(String, Int)] = TagsKword.makeTags(row,broadvalues)

      (userId,adList++appList++businessList++devList++locList++kwList)

    }).rdd.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).foreach(println)


  }
}
