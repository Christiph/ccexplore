package com.kunyan.newsvisit

import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2017/1/13.
  */
object Run {

  def main (args: Array[String]): Unit = {

    //屏蔽spark日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("NewsVisit_Run")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val jsonConfig = new JsonConfig
    jsonConfig.initConfig(args(0))
    val partitionNum = args(1).toInt
    val teleDataPath = args(2) + "*" //电信数据的路径
    val outputPath = args(3)  //结果保存的路径

    val news = NewsVisitComputing.readNewsData(jsonConfig, sqlContext, partitionNum)

    val tele = NewsVisitComputing.readTeleData(sc, teleDataPath)

    val result = NewsVisitComputing.joinTwoTable(news, tele)

    result.coalesce(1).saveAsTextFile(outputPath)

    sc.stop()
  }
}
