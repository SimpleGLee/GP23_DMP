package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 17-15
  * 地域分布
  */
object AreaDist {
  def main(args: Array[String]): Unit = {
    //判断是否有输入路径
    if(args.length != 1){
      println("目录路径不正确")
      sys.exit()
    }
    //指定输入路径参数
    val Array(inputPath) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AreaDist")
      .master("local[2]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet(inputPath)
    //注册临时表
    df.createTempView("log")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame: DataFrame = df.select(df.col("provincename"),
      df.col("cityname"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0).alias("oldReq"),
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0).alias("effReq"),
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0).alias("adReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("joinReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"iswin" === 1 && $"adorderid" != 0, 1).otherwise(0).alias("succReq"),
      when($"requestmode" === 2 && $"iseffective" === 1, 1).otherwise(0).alias("showCount"),
      when($"requestmode" === 3 && $"iseffective" === 1, 1).otherwise(0).alias("clickCount"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspconsume"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspCost")
    ).groupBy("provincename", "cityname").agg(
      sum("oldReq").as("oldReqSum"),
      sum("effReq").as("effReqSum"),
      sum("adReq").as("adReqSum"),
      sum("joinReq").as("joinReqSum"),
      sum("succReq").as("succReqSum"),
      sum("succReq").cast("Double")/sum("joinReq").alias("joinsucc"),//竞价成功率
      sum("showCount").as("showCountSum"),
      sum("clickCount").as("clickCountSum"),
      sum("clickCount").cast("Double")/sum("showCount").alias("clicklv"),//点击率
      sum("dspconsume").as("dspconsumeSum") / 1000,
      sum("dspCost").as("dspCostSum") / 1000
    )
    frame.show()

    spark.stop()

  }
}
