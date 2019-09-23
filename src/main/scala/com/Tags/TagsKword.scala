package com.Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Write By SimpleLee 
  * On 2019-九月-星期日
  * 17-40
  */
object TagsKword extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    val stopword: Broadcast[collection.Map[String, Int]] = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    //取值判断
    row.getAs[String]("keywords").split("\\|")
      .filter(word=>word.length>=3 && word.length<=8 && !stopword.value.contains(word))
      .foreach(word=>list:+=("K"+word,1))

    list
  }
}
