package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 10-47
  * 从高德地图获取商圈信息
  */
object AmapUtil {
  /**
    * 解析经纬度
    * @param long
    * @param lat
    */
  def getBusinessFromAmap(long:Double,lat:Double):String={
    //"https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=0802edbff99345213340e64730f701c0&radius=1000&extensions=all
    val location = long+","+lat
    //获取url
    val url = s"https://restapi.amap.com/v3/geocode/regeo?location=$location&key=0802edbff99345213340e64730f701c0&radius=1000&extensions=all"
    //调用Http接口发送请求
    val jsonstr: String = HttpUtil.get(url)
    //解析json串
    val jSONObject: JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val status: Int = jSONObject.getIntValue("status")
    if(status == 0) return ""
    //如果不为空
    val jSONObject1: JSONObject = jSONObject.getJSONObject("regeocode")
    if(jSONObject1 == null) return ""
    val jSONObject2: JSONObject = jSONObject1.getJSONObject("addressComponent")
    if(jSONObject2 == null) return ""
    val jSONArray: JSONArray = jSONObject2.getJSONArray("businessAreas")
    if(jSONArray == null) return ""

    //定义集合取值
    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    //循环数组
    for(item <- jSONArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        //将数组元素转为json
        val json: JSONObject = item.asInstanceOf[JSONObject]
        //取出json中的具体字段
        val name: String = json.getString("name")
        //将取到的商圈名称追加到集合中
        result.append(name)
      }
    }

    //商圈名字
    result.mkString(",")
  }

}
