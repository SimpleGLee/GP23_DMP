package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Write By SimpleLee 
  * On 2019-九月-星期日
  * 17-34
  * 地域标签
  */
object TagsLocation extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    //获取数据类型
    val row: Row = args(0).asInstanceOf[Row]
    //获取地域数据
    val pro: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")
    if(StringUtils.isNoneBlank(pro)){
      list:+=("ZP"+pro,1)
    }else{
      list:+=("ZC"+city,1)
    }

    list
  }
}
