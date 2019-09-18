package com.Location

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 19-46
  * 终端设备
  */
object TerminalDist {

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
      .master("local[*]")
      //设置序列化级别
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet(inputPath)
    //注册临时表
    df.createTempView("log")

    //终端设备-运营
    Operator(df,spark)

    //终端设备-网络类
//    NetWorkClass(df,spark)

    //终端设备-设备类
//    EquipmentClass(df,spark)

    //终端设备-操作系统
//    OperatingSystem(df,spark)

    spark.stop()
  }
  def Operator(df:DataFrame,spark:SparkSession): Unit ={
    //终端设备-运营
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame1: DataFrame = df.select(df.col("ispid"),
      df.col("ispname"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0).alias("oldReq"),
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0).alias("effReq"),
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0).alias("adReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("joinReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"iswin" === 1 && $"adorderid" != 0, 1).otherwise(0).alias("succReq"),
      when($"requestmode" === 2 && $"iseffective" === 1, 1).otherwise(0).alias("showCount"),
      when($"requestmode" === 3 && $"iseffective" === 1, 1).otherwise(0).alias("clickCount"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspconsume"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspCost")
    ).groupBy("ispid","ispname").agg(
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
    ).orderBy("ispid")
    frame1.show()
  }

  //终端设备-网络类
  def NetWorkClass(df: DataFrame, spark: SparkSession) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame: DataFrame = df.select(df.col("networkmannerid"),
      df.col("networkmannername"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0).alias("oldReq"),
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0).alias("effReq"),
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0).alias("adReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("joinReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"iswin" === 1 && $"adorderid" != 0, 1).otherwise(0).alias("succReq"),
      when($"requestmode" === 2 && $"iseffective" === 1, 1).otherwise(0).alias("showCount"),
      when($"requestmode" === 3 && $"iseffective" === 1, 1).otherwise(0).alias("clickCount"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspconsume"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspCost")
    ).groupBy("networkmannerid","networkmannername").agg(
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
    ).orderBy("networkmannerid")
    frame.show()
  }

  //终端设备-设备类
  def EquipmentClass(df: DataFrame, spark: SparkSession) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame: DataFrame = df.select(df.col("devicetype"),
      when($"devicetype"===1,"手机").when($"devicetype"===2,"平板").otherwise("其他").alias("devicename"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0).alias("oldReq"),
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0).alias("effReq"),
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0).alias("adReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("joinReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"iswin" === 1 && $"adorderid" != 0, 1).otherwise(0).alias("succReq"),
      when($"requestmode" === 2 && $"iseffective" === 1, 1).otherwise(0).alias("showCount"),
      when($"requestmode" === 3 && $"iseffective" === 1, 1).otherwise(0).alias("clickCount"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspconsume"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspCost")
    ).groupBy("devicetype").agg(
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
    ).orderBy("devicetype")
    frame.show()
  }

  //终端设备-操作系统
  def OperatingSystem(df: DataFrame, spark: SparkSession) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val frame: DataFrame = df.select(df.col("client"),
      when($"client"===1,"Andriod").when($"client"===2,"IOS").otherwise("其他").alias("clientname"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0).alias("oldReq"),
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0).alias("effReq"),
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0).alias("adReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("joinReq"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"iswin" === 1 && $"adorderid" != 0, 1).otherwise(0).alias("succReq"),
      when($"requestmode" === 2 && $"iseffective" === 1, 1).otherwise(0).alias("showCount"),
      when($"requestmode" === 3 && $"iseffective" === 1, 1).otherwise(0).alias("clickCount"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspconsume"),
      when($"iseffective" === 1 && $"isbilling" === 1 && $"isbid" === 1, 1).otherwise(0).alias("dspCost")
    ).groupBy("client","clientname").agg(
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
    ).orderBy("client")
    frame.show()
  }

}
