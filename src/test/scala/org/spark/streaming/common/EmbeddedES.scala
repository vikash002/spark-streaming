package org.spark.streaming.common

import java.io.File

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClients
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.spark.streaming.common.sparkjob.config.{Configuration, TestConfiguration}

trait EmbeddedES extends FunSuite with BeforeAndAfterAll {

  private val esHost: String = "127.0.0.1"
  private val esPort: String = "9200"
  private val esTransportPort: String = "9300"
  private var esWorkingDir: File = _
  private var esNode: Node = _
  protected val esIndex: String = "insights_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    esNode = createEmbeddedESNode()
    esNode.start()
    TestConfiguration.withConfiguration(Map(Configuration.EsIndex -> esIndex))
  }

  override def afterAll(): Unit = {
    dropESIndex()
    esNode.close()
    super.afterAll()
  }

  private def createEmbeddedESNode(): Node = {
    initializeWorkingDir()
    val settings = Settings.builder()
      .put("path.home", esWorkingDir.getAbsolutePath)
      .put("path.conf", esWorkingDir.getAbsolutePath)
      .put("path.data", esWorkingDir.getAbsolutePath)
     // .put("path.work", esWorkingDir.getAbsolutePath)
      .put("path.logs", esWorkingDir.getAbsolutePath)
      .put("http.port", esPort)
      .put("transport.tcp.port", esTransportPort)
      .put("transport.type" , "local")
     // .put("index.number_of_shards", "1")
     // .put("index.number_of_replicas", "0")
      .build()
    new Node(settings)
  }

  private def initializeWorkingDir() = {
    esWorkingDir = File.createTempFile("es-temp", System.nanoTime().toString)
    esWorkingDir.delete()
    esWorkingDir.mkdir()
  }

  protected def dropESIndex() = {
    val httpClient = HttpClients.createDefault()
    val httpDelete = new HttpDelete("http://%s:%s/%s".format(esHost, esPort, esIndex))
    val response = httpClient.execute(httpDelete)
    response.close()
  }
}
