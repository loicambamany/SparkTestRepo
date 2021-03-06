package org.com.cigna.loic.spark


import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.FlatSpec
import  org.scalatest.BeforeAndAfterEach



class TestSparkSpec  extends FlatSpec with BeforeAndAfterEach {

  var sparkSession : SparkSession = _

  val spark = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  /*test("your test name here"){
    //your unit test assert here like below
    assert("True".toLowerCase ==  "true")
  }*/

  override def afterEach() {
    spark.stop()
  }

}
