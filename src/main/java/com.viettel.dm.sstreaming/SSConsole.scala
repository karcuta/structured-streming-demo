package com.viettel.dm.sstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SSConsole {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    System.setProperty("hadoop.home.dir", "D:\\BDA\\Project\\demostructuredstreaming\\hadoop_local\\winutils")
    // System.setProperty("hadoop.home.dir", "hadoop_local\\winutils")
    val conf = new SparkConf().setAppName("StructuredConsole")
    conf.set("spark.default.parallelism", "4")
    conf.setMaster("local[2]")
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val streamDF = spark.readStream
      .format("socket")
      .option("host", "103.226.251.23")
      .option("port", 8881)
      .load()

    streamDF.printSchema()

    val wordCounts = streamDF.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(10000))
      .format("console")
      .start()

    query.awaitTermination()

  }

}
