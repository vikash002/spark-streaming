/*
package org.spark.streaming.common

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.spark.streaming.common.sparkjob.config.TestConfiguration._
import redis.embedded.RedisExecProvider._
import redis.embedded.RedisServer
import redis.embedded.util.JarUtil._
import redis.embedded.util.OS

trait EmbeddedRedis extends FunSuite with BeforeAndAfterAll {

  protected var redisServer: RedisServer = _
  //protected var redisCacheClient = _

  override def beforeAll() {
    super.beforeAll()
    withConfiguration(Map[String, String](
    ))
    val redisServerPath = extractExecutableFromJar("redis-server-jenkins-2.8.19").getAbsolutePath
    redisServer = new RedisServer(defaultProvider.`override`(OS.UNIX, redisServerPath), 6379)
    redisServer.start()
    //redisCacheClient = new RedisCacheClient(redisHost, redisPort, cacheExpireTimeInSec)
  }

  protected def dropRedisDB() = {
//    val jedis = new Jedis(redisHost, redisPort)
//    jedis.flushDB()
//    jedis.close()
  }

  override def afterAll() {
    //redisCacheClient.destroy()
    redisServer.stop()
    super.afterAll()
  }
}
*/
