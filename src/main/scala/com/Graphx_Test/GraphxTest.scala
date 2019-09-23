package com.Graphx_Test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Write By SimpleLee 
  * On 2019-九月-星期一
  * 10-34
  */
object GraphxTest {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Graphx")
      .master("local")
      .getOrCreate()

    //创建点和边
    //构建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sparkSession.sparkContext.makeRDD(Seq(
      (1L, ("小明", 26)),
      (2L, ("小红", 25)),
      (6L, ("小黑", 12)),
      (9L, ("小白", 25)),
      (133L, ("小黄", 21)),
      (138L, ("小吕", 28)),
      (158L, ("小将", 29)),
      (44L, ("小新", 20)),
      (21L, ("小Q", 23)),
      (5L, ("小W", 19)),
      (7L, ("小E", 18))
    ))

    //构建边的集合
    val edgeRDD: RDD[Edge[Int]] = sparkSession.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构建图
    val graphx = Graph(vertexRDD,edgeRDD)

    //取顶点
    val vertices: VertexRDD[VertexId] = graphx.connectedComponents().vertices

    //匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age))) => (cnId,List((name,age)))
    }.reduceByKey(_++_)
      .foreach(println)

  }
}
