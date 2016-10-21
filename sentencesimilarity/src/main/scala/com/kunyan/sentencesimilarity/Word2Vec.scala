package com.kunyan.sentencesimilarity

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2Vec

/**
  * Created by wangcao on 2016/10/20.
  * 训练word2vec,保存语料库词向量
  */
object Word2Vec {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WORD2VEC")//.setMaster("local")
    val sc = new SparkContext(conf)

    //读取分词后的语料库
    val text = sc.textFile(args(0)).map(_.split("\t")).filter(x => x.length == 4).map(x => x(3))

    //预处理：去掉所有英文，数字
    val segWord = text.map(_.split(",").toSeq)
      .map(x => x.filter(a => a != "")
        .filter(a => !a.matches("[a-zA-Z.' ]+"))
        .filter(a => !a.matches(".*[0-9]+.*")))

    //训练word2vec模型
    val word2vec = new Word2Vec()
    word2vec.setSeed(11L).setMinCount(10)
    val model = word2vec.fit(segWord)
    val vec = sc.parallelize(model.getVectors.map(x => (x._1, x._2.mkString(" "))).toSeq)

    //保存词向量
    vec.saveAsTextFile(args(1))

    sc.stop()
  }

}
