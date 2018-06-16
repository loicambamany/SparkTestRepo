package org.com.cigna.loic.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.mutable


class AppTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = null
  val filterWords = "file:///tmp/test_folder/words"


  override def beforeEach(): Unit = {

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")

//    sc = new SparkContext("local[2]", "unit test", sparkConfig)

//    val conf = new SparkConf().setMaster("local[2]").setAppName("AppTest")
//    sc = new SparkContext(conf)


  }

  override def afterEach() {
    sc.stop()
  }

  test("Test word count") {
    //val filterWordsBC = sc.broadcast(scala.io.Source.fromFile(filterWords).getLines())
    val filterWordsBC = sc.textFile("file:///tmp/test_folder/words")

    val quotesRDD = sc.parallelize(Seq("Courage is not simply one of the virtues, but the form of every virtue at the testing point",
      "We have a very active testing community which people don't often think about when you have open source",
      "Program testing can be used to show the presence of bugs, but never to show their absence",
      "Simple systems are not feasible because they require infinite testing",
      "Testing leads to failure, and failure leads to understanding"))

    val filterWordsCounts = quotesRDD.flatMap(_.split(' ')).
      map(r => (r.toLowerCase, 1)).
      reduceByKey((a,b) => a + b)

    filterWordsCounts.collect.foreach(println)

    val wordMap = new mutable.HashMap[String, Int]()

    filterWordsCounts.take(100).foreach { case (word, count) => wordMap.put(word, count) }

    assert(wordMap.get("to").get == 4, "The word count for 'to' should had been 4 but it was " + wordMap.get("to").get)
    assert(wordMap.get("testing").get == 5, "The word count for 'testing' should had been 5 but it was " + wordMap.get("testing").get)
    assert(wordMap.get("is").get == 1, "The word count for 'is' should had been 1 but it was " + wordMap.get("is").get)
    assert(wordMap.get("the").get == 4, "The word count for 'the' should had been 1 but it was " + wordMap.get("the").get)

  }

}
