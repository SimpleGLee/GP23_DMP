package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import com.util.TagsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 09-49
  * 上下文的标签主类
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputPath,docs,stopwords,day) = args

    //创建Spark上下文
    val spark: SparkSession = SparkSession.builder().appName("TagContext").master("local[2]").getOrCreate()
    import spark.implicits._


    /**
      * 调用HbaseAPI
      */
    val load: Config = ConfigFactory.load()
    //获取表名
    val HbaseTableName: String = load.getString("HBASE.tableName")
    //创建Hadoop任务
    val configuration: Configuration = spark.sparkContext.hadoopConfiguration

    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    //获取connection连接
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)
    val hbadmin: Admin = hbConn.getAdmin
    //判断当前表是否被使用
    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
      println("当前表可用")

      //创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))

      //创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")

      //将创建好的列簇加入表中
      tableDescriptor.addFamily(hColumnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbConn.close()
    }

    val conf = new JobConf(configuration)
    //指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出那张表
    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)



    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)

    //读取字典文件--为了给APP名称打标签
    val docsRDD: collection.Map[String, String] = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5).map(arr=>
      (arr(4), arr(1))).collectAsMap()
    //广播字典
    val broadvalue: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docsRDD)


    //读取停用字典文件--为了给APP名称打标签
    val stopwordsRDD: collection.Map[String, Int] = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()
    //广播字典
    val broadvalues: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopwordsRDD)


    //处理数据信息
    val allUserId: RDD[(List[String], Row)] = df.rdd.map(row => {
      //获取一个用户所有的ID
      val strList: List[String] = TagsUtils.GetAllUserId(row)
      (strList, row)
    })
//    allUserId.foreach(println)

    //构建点集合
    val verties: RDD[(Long, List[(String, Int)])] = allUserId.flatMap(row => {
      //获取所有数据
      val rows: Row = row._2
      val adList: List[(String, Int)] = TagsAd.makeTags(rows)
      //商圈
      val businessList: List[(String, Int)] = BusinessTag.makeTags(rows)
      //媒体标签
      val appList: List[(String, Int)] = TagsAPP.makeTags(rows, broadvalue)
      //设备标签
      val devList: List[(String, Int)] = TagsDevice.makeTags(rows)
      //地域标签
      val locList: List[(String, Int)] = TagsLocation.makeTags(rows)
      //关键字标签
      val kwList: List[(String, Int)] = TagsKword.makeTags(rows, broadvalues)
      //获取所有的标签
      val tagList: List[(String, Int)] = adList++appList++devList++locList++kwList
      //保留用户ID
      val VD = row._1.map((_, 0))++tagList

      /**
        * 思考：
        *   1.如何保证其中一个ID携带着用户的标签
        *   2.用户ID的字符串如何处理
        */
      row._1.map(uId => {
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })

    })

    //打印
    verties.take(20).foreach(println)
    //构建边的集合
    val edges: RDD[Edge[Int]] = allUserId.flatMap(row => {
      row._1.map(uId =>Edge(row._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })
    edges.foreach(println)

    //构建图
    val graph = Graph(verties,edges)

    /**
      * 根据图计算中的连通图算法，通过图中的分支，连通所有的点
      * 然后再根据所有点，找到内部最小的点，为当前的公共点
      */
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    })//这里是聚合后的结果
      .map{
      case (userId,userTags)=>{
        //设置rowkey和列，列名
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),
          Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)//TODO:这里是HBASE上的结果数据

    spark.stop()
  }
}
