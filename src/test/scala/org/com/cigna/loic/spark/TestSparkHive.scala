package org.com.cigna.loic.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import  org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.Row


class TestSparkHive extends FunSuite with BeforeAndAfterEach {


  var sparkSession: SparkSession = _
  @transient var sc: SparkContext = null

  case class Person(name: String, time: Int, amount: Int)

  val warehouseLocation = "file:///tmp/test_folder/spark-warehouse"

  val spark = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master("local")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("your test name here") {

    val data = Seq(("ted", 1, 3), ("JR", 1, 10), ("Loic", 1, 11), ("Sam", 1, 1))

    val personData = data.map(l => Person(l._1,l._2,l._3)
    )

    val personDF = spark.createDataFrame(personData).toDF()

    val personNewDF = personDF.withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "account")
      .withColumnRenamed("_3", "amount")

    personNewDF.registerTempTable("PersonInfos")
    val resultsDF = spark.sql("SELECT * FROM PersonInfos")
    resultsDF.show(4)

    val sum_Persons = spark.sql("SELECT SUM(amount) FROM PersonInfos")
    sum_Persons.show()

    val localAgeSum = sum_Persons.take(10)
    assert(localAgeSum(0).get(0) == 25, "The sum of age should equal 25 but it equaled " + localAgeSum(0).get(0))
  }

  override def afterEach() {
    spark.stop()

  }

}
