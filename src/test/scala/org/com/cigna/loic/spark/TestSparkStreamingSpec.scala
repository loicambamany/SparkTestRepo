package org.com.cigna.loic.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterEach

class TestSparkStreamingSpec extends FlatSpec  with BeforeAndAfterEach {

  var sparkSession : SparkSession = _


  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  val spark = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  override def beforeEach() {
    /*sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()*/

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

 /* test("your test name here"){
    //your unit test assert here like below
    assert("True".toLowerCase ==  "true")
  }*/

  override def afterEach() {
    spark.stop()
  }


}
