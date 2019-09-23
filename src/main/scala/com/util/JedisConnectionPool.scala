package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Write By SimpleLee 
  * On 2019-九月-星期四
  * 14-12
  * Redis连接
  */
object JedisConnectionPool {
  val config = new JedisPoolConfig()
  //设置最大连接数
  config.setMaxTotal(20)
  //设置最大等待空闲数
  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"hadoop01",6379,10000)

  def getConnection():Jedis={
    pool.getResource
  }

}
