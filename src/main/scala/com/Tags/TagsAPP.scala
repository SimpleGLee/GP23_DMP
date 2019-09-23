package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Write By SimpleLee 
  * On 2019-九月-星期日
  * 16-53
  * 媒体标签
  */
object TagsAPP extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]
    val appdoc: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    //获取APPname，APPid
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    if(StringUtils.isNoneBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=("APP"+appdoc.value.getOrElse(appid,"其他"),1)
    }

    list
  }
}
