package com.kunyan.sentencerank

//import breeze.linalg.Vector
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/11/17.
  */
object test {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\Administrator\\Desktop\\test\\title.txt")
      .map(_.split(",")).collect
    val path = "hdfs://61.147.114.85:9000/home/word2vec/model-10-100-20/2016-08-16-word2VectorModel/"

    val result = SentenceRank.run(sc, data, path, 100, "hehe", 2, 100, 0.9F)
    println(result)


  }

}
