package com.util

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 09-46
  * 处理指标工具类
  */
//val requestmode: Int = row.getAs[Int]("requestmode")
//val processnode: Int = row.getAs[Int]("processnode")
//val iseffective: Int = row.getAs[Int]("iseffective")
//val isbilling: Int = row.getAs[Int]("isbilling")
//val isbid: Int = row.getAs[Int]("isbid")
//val iswin: Int = row.getAs[Int]("iswin")
//val adorderid: Int = row.getAs[Int]("adorderid")
//val winprice: Double = row.getAs[Double]("winprice")//竞价成功
//val adpayment: Double = row.getAs[Double]("adpayment")//竞价支付成本

object ReqUtils {

  // 处理请求数
  def ReqAreaProCity(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  //处理点击展示数
  def ClickAreaProCity(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }

  //处理竞价，成功广告产品和消费
  def AdSuccAreaProCity(iseffective:Int,isbilling:Int,isbid:Int,
                        iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective == 1 && isbilling == 1 && isbid == 1){
      if(iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
}
