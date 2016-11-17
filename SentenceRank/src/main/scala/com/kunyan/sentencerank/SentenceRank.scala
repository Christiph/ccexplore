package com.kunyan.sentencerank

import org.apache.spark.SparkContext

/**
  * Created by wangcao on 2016/11/17.
  */
object SentenceRank {

  /**
    * 提取关键句
    *
    * @param sc SparkContext
    * @param sentence 所有句子的集合
    * @param path Wprd2Vec模型保存的路径
    * @param size 词向量的大小
    * @param graphName 图标识
    * @param keySentenceNum 要提取的关键句的个数
    * @param iterator 循环的次数
    * @param df 阻尼系数
    * @return （关键句， 权重）
    */
  def run(sc: SparkContext,
          sentence: Array[Array[String]],
          path: String,
          size: Int,
          graphName: String,
          keySentenceNum: Int,
          iterator: Int,
          df: Float): List[(Array[String], Float)] = {

    val sentenceWithIndex = sentence.zipWithIndex.map(x => (x._2, x._1))
    val sentenceMap = sentenceWithIndex.toMap

    val graph = BuildGraph.finalGraph(sc, sentenceWithIndex, path, size, graphName)

    //输出提取的关键词
    val keywordExtractor = new PropertyExtractor(graph, keySentenceNum)
    val result = keywordExtractor.extractKeywords(iterator, df)
      .map(x => (sentenceMap(x._1.toInt), x._2))

    result
  }

}
