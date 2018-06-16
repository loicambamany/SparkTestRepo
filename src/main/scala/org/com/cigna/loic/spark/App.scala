package org.com.cigna.loic.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveMetastoreCatalog
//import  org.apache.hadoop.fs.FSDataInputStream

object App {

  def main(args:Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkExample")
    val sc = new SparkContext(conf)


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val location = ""
    val spark = SparkSession
      .builder()
      .appName("")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()


      val file = sc.textFile("file:///tmp/test_folder/transactions")

      val file_split = file.map(_.split("\t"))
      val file_map = file_split.map(l => (l(0).toInt,l(1).toInt,l(2)))
      val file_sort = file_map.sortBy(_._2)

      val file_mapsort = file_sort.map(l => {
        val v1 = l._2
        val v2 = l._1
        (v1,v2)
      })

    file_mapsort.collect().foreach(println)

    val file_reduce = file_mapsort.reduceByKey(_+_)

    file_reduce.collect().foreach(println)


  }

}

