package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunJob extends App {
  def main(args: Array[String]) {
    val logFile = "/Users/harishbohara/workspace/personal/spark/src/test/java/org/example/AppTest.java"


    val scc = new SparkConf();
    scc.setMaster("local")
    scc.setAppName("a")
    /*
    val sparkSession: SparkSession = SparkSession.builder()
      .config(scc)
      .getOrCreate()*/

    val s = SparkSession.builder()
      .master("local")
      .getOrCreate()

    // val sparkContext = sparkSession.sparkContext


    val logData = s.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    s.stop()
  }
}
