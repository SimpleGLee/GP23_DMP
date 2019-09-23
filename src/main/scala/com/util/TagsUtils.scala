package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 09-54
  */
object TagsUtils {

  //获取用户ID
  def GetOneUserId(row:Row):String = {
    row match {
      case t if StringUtils.isNoneBlank(t.getAs[String]("imei")) => "IM"+t.getAs[String]("imei")
      case t if StringUtils.isNoneBlank(t.getAs[String]("mac")) => "MA"+t.getAs[String]("mac")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfa")) => "ID"+t.getAs[String]("idfa")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudid")) => "OD"+t.getAs[String]("openudid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androidid")) => "AD"+t.getAs[String]("androidid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeimd5")) => "IM"+t.getAs[String]("imeimd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macmd5")) => "MA"+t.getAs[String]("macmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfamd5")) => "ID"+t.getAs[String]("idfamd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidmd5")) => "OD"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididmd5")) => "AD"+t.getAs[String]("androididmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeisha1")) => "IM"+t.getAs[String]("imeisha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macsha1")) => "MA"+t.getAs[String]("macsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfasha1")) => "ID"+t.getAs[String]("idfasha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidsha1")) => "OD"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididsha1")) => "AD"+t.getAs[String]("androididsha1")
      case _ => "其他"
    }
  }

  //获取一个用户的全部ID
  def GetAllUserId(v:Row):List[String]={
    var list = List[String]()
    v match {
      case t if StringUtils.isNoneBlank(t.getAs[String]("imei")) => list:+="IM"+t.getAs[String]("imei")
      case t if StringUtils.isNoneBlank(t.getAs[String]("mac")) => list:+="IM"+t.getAs[String]("mac")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfa")) => list:+="IM"+t.getAs[String]("idfa")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudid")) => list:+="IM"+t.getAs[String]("openudid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androidid")) => list:+="IM"+t.getAs[String]("androidid")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeimd5")) => list:+="IM"+t.getAs[String]("imeimd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macmd5")) => list:+="IM"+t.getAs[String]("macmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfamd5")) => list:+="IM"+t.getAs[String]("idfamd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidmd5")) => list:+="IM"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididmd5")) => list:+="IM"+t.getAs[String]("androididmd5")
      case t if StringUtils.isNoneBlank(t.getAs[String]("imeisha1")) => list:+="IM"+t.getAs[String]("imeisha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("macsha1")) => list:+="IM"+t.getAs[String]("macsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("idfasha1")) => list:+="IM"+t.getAs[String]("idfasha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("openudidsha1")) => list:+="IM"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNoneBlank(t.getAs[String]("androididsha1")) => list:+="IM"+t.getAs[String]("androididsha1")

    }
    list
  }
}
