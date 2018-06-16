package org.com.cigna.loic.spark


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import  org.scalatest.BeforeAndAfterEach

import org.apache.spark.util.Utils

import java.io.File
import java.sql.Timestamp

import com.google.common.io.Files
import org.apache.hadoop.fs.FileSystem


class TestSparkPartitionSuite  extends FunSuite with BeforeAndAfterEach {

  //import spark.implicits._


  var sparkSession: SparkSession = _
  @transient var sc: SparkContext = null

  case class Person(name: String, time: Int, amount: Int)

  case class Entry(id: Int, amount: Int)
  val tmpDir = Files.createTempDir()

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

    val TestData = Seq((1, 3), (1, 10), (1, 11), (1, 1))

    //          val testData = sc.parallelize(
    //            (1 to 10).map(i => TestData(i)))
    val testData = TestData.map(l => Entry(l._1,l._2))
    val peopleDataFrame = spark.createDataFrame(testData).toDF()
    peopleDataFrame.createOrReplaceTempView("testData")

    val peopleNewDF = peopleDataFrame.withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "amount")

    peopleNewDF.registerTempTable("peopleInfos")

    // create the table for test
    spark.sql(s"CREATE TABLE table_with_partition8(id int,amount int) " +
      s"PARTITIONED by (ds string) location '${tmpDir}' ")
    spark.sql("INSERT OVERWRITE TABLE table_with_partition8  partition (ds='1') " +
      "SELECT id,amount FROM peopleInfos")
    spark.sql("INSERT OVERWRITE TABLE table_with_partition8  partition (ds='2') " +
      "SELECT id,amount FROM peopleInfos")
    spark.sql("INSERT OVERWRITE TABLE table_with_partition8  partition(ds='3') " +
      "SELECT id,amount FROM peopleInfos")
    spark.sql("INSERT OVERWRITE TABLE table_with_partition8  partition (ds='4') " +
      "SELECT id,amount FROM peopleInfos")



   /*  // test for the exist path
     checkAnswer(sql("select key,value from table_with_partition"),
       testData.union(testData).union(testData).union(testData))
*/
     // delete the path of one partition
     tmpDir.listFiles
       .find { f => f.isDirectory && f.getName().startsWith("ds=") }
       .foreach { f =>  f.delete()}

    // Utils.deleteRecursively(f)

    /* // test for after delete the path
     checkAnswer(sql("select key,value from table_with_partition"),
       testData.union(testData).union(testData))
*/


    //  }
    //  }
    //  }
  }

  test("SPARK-21739: Cast expression should initialize timezoneId") {
    //withTable("table_with_timestamp_partition") {
    spark.sql("CREATE TABLE table_with_timestamp_partition3(amount int) PARTITIONED BY (ts TIMESTAMP)")
    spark.sql("INSERT OVERWRITE TABLE table_with_timestamp_partition3 " +
      "PARTITION (ts = '2010-01-01 00:00:00.000') VALUES (1)")
  }

  override def afterEach() {
    spark.stop()

  }

  /*

  test("SPARK-5068: query data when path doesn't exist") {
    withSQLConf(SQLConf.HIVE_VERIFY_PARTITION_PATH.key -> "true") {
      queryWhenPathNotExist()
    }
  }

  test("Replace spark.sql.hive.verifyPartitionPath by spark.files.ignoreMissingFiles") {
    withSQLConf(SQLConf.HIVE_VERIFY_PARTITION_PATH.key -> "false") {
      sparkContext.conf.set(IGNORE_MISSING_FILES.key, "true")
      queryWhenPathNotExist()
    }
  }

  */

  /*test("SPARK-21739: Cast expression should initialize timezoneId") {
    //withTable("table_with_timestamp_partition") {
      spark.sql("CREATE TABLE table_with_timestamp_partition1(amount int) PARTITIONED BY (ts TIMESTAMP)")
      spark.sql("INSERT OVERWRITE TABLE table_with_timestamp_partition1 " +
        "PARTITION (ts = '2010-01-01 00:00:00.000') VALUES (1)")
*/
    /*  // test for Cast expression in TableReader
      checkAnswer(sql("SELECT * FROM table_with_timestamp_partition"),
        Seq(Row(1, Timestamp.valueOf("2010-01-01 00:00:00.000"))))

      // test for Cast expression in HiveTableScanExec
      checkAnswer(sql("SELECT value FROM table_with_timestamp_partition " +
        "WHERE ts = '2010-01-01 00:00:00.000'"), Row(1))*/
  //  }
 // }
}
