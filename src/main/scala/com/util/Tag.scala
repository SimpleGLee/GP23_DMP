package com.util

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 10-18
  * 打标签接口
  */
trait Tag {

  def makeTags(args:Any*):List[(String,Int)]

}
