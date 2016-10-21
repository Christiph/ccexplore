package com.kunyan.sentencesimilarity

import breeze.linalg.max
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by wangcao on 2016/10/21.
  *
  * 利用余弦相似性重构句向量，计算句向量间余弦相似性
  */
object ComputeCorrByCorr {

  /**
    * 构建句子的向量
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param wordStay 在词向量中存在的句子中的词
    * @return 句向量
    */
  def sentence2Vec(sc: SparkContext,
                   vec: RDD[(String, Array[(Double)])],
                   wordStay: Array[String]): RDD[(String,Double)] = {

    var wordCorrelation: RDD[(String,  Double)] = sc.parallelize(Array(null))

    //将每个词与词向量中的词分别两两做余弦相思性计算
    for (word <- wordStay) {

      val selectWord = vec.filter(x => x._1 == word).collect().toMap
      val wordVec = selectWord(word).toVector

      val wordCorr = vec.map(x => {

        val singleVec = x._2.toVector
        val corr = Correlation.cosineCorr(singleVec, wordVec)

        (x._1, corr)
      })

      wordCorrelation = wordCorrelation.union(wordCorr)

    }

    //取最高的相似性作为该句子在该词向量维度上的值
    val sentenceCorr = wordCorrelation.filter(x => x != null)
      .groupByKey()
      .map(x => (x._1, max(x._2)))
      .sortBy(x => x._1)

    sentenceCorr
  }


  /**
    * 运行计算句向量
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param text 句子
    * @return 句向量
    */
  def run(sc: SparkContext, vec: RDD[(String, Array[(Double)])], text: String): RDD[(String,Double)] = {

    //取出词典中的词列表
    val dicList = vec.map(_._1).collect()
    //输入文本根据逗号分隔转换成Array
    val wordArray = text.split(",")
    //词列表中有的词保留，没有的词离开（打印出离开的词）
    val wordStay = wordArray.filter(x => dicList.contains(x))
    val wordLeave = wordArray.filter(x => !wordStay.contains(x))
    println(wordLeave.length + " words are removed:" + wordLeave.mkString(","))
    //剩下词的个数
    val wordNum = wordStay.length

    //如果次数为0，则打印提示并返回null，如果大于0，则继续计算，返回句向量
    if (wordNum == 0) {

      println("the sentence:" + text + "has no word in dictionary!")

      sc.parallelize(Array(null))
    } else {

      sentence2Vec(sc, vec, wordStay)
    }

  }

  /**
    * 计算两个句子间的相似度
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param text1 句子1
    * @param text2 句子1
    * @return
    */
  def sentenceCorr(sc: SparkContext,
                   vec: RDD[(String, Array[(Double)])],
                   text1: String, text2: String):Double = {

    val score1 = run(sc, vec, text1)
    val score2 = run(sc, vec, text2)
    val sentenceCorr = Correlation.cosineCorr(score1, score2)

    sentenceCorr
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ComputeCorrByCorr").setMaster("local")
    val sc = new SparkContext(conf)

    //读入word2vec训练好的词向量
    val vec = sc.textFile("hdfs://61.147.114.85:9000/home/word2vec/model-10-100-20/2016-08-16.txt")
     // .map(x => x.replace("(", "")).map(x => x.replace(")", ""))
      .map(_.split("\t")).map(x => (x(0), x(1).split(",").map(_.toDouble))).cache()

    //文本
    val text1 = "妻子,法庭,提供,出轨,证据"
    val text2 = "钟爱,绿都地产,石油,价格,上涨"

    //计算
    println(sentenceCorr(sc, vec,text1, text2))

    sc.stop()

 }

}
