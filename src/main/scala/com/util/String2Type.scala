package com.util

/**
  * Write By SimpleLee 
  * On 2019-九月-星期二
  * 10-33
  * 类型转换工具类
  */
object String2Type {

  def toInt(str:String):Int={
    try{
      str.toInt
    }catch{
      case _:Exception => 0
    }
  }
  def toDouble(str:String):Double={
    try{
      str.toInt
    }catch{
      case _:Exception => 0.0
    }
  }
}
