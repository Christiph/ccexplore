package com.kunyan.sentencesimilarity

import org.apache.spark.rdd.RDD

/**
  * Created by wangcao on 2016/10/21.
  */
object Correlation {

  /**
    * 计算余弦相似度（vector形式）
    *
    * @param vec1 词向量
    * @param vec2 词向量
    * @return 余弦相似度的值
    */
  def cosineCorr(vec1: Vector[Double], vec2: Vector[Double]): Double = {

    val member = vec1.zip(vec2).map(x => x._1 * x._2).sum
    val tmp1 =  math.sqrt(vec1.map(num => {math.pow(num, 2)}).sum)
    val tmp2 =  math.sqrt(vec2.map(num => {math.pow(num, 2)}).sum)

    member / (tmp1 * tmp2)
  }

  /**
    * 计算余弦相似度（RDD形式）
    *
    * @param vec1 RDD[(词或索引为key，对应的value)]
    * @param vec2 RDD[(词或索引为key，对应的value)]
    * @return 余弦相似度的值
    */
  def cosineCorr(vec1: RDD[(String, Double)], vec2: RDD[(String, Double)]): Double = {

    val member = vec1.join(vec2).map(x => x._2._1 * x._2._2).sum
    val tmp1 =  math.sqrt(vec1.map(num => {math.pow(num._2, 2)}).sum)
    val tmp2 =  math.sqrt(vec2.map(num => {math.pow(num._2, 2)}).sum)

    member / (tmp1 * tmp2)
  }

  /**
    * 计算欧式距离（vector形式）
    *
    * @param vec1 词向量
    * @param vec2 词向量
    * @return 欧氏距离相似性
    */
  def pearsonCorr(vec1: Vector[Double], vec2: Vector[Double]): Double = {

    math.sqrt(vec1.zip(vec2).map(x => math.pow(x._1 - x._2, 2)).sum)
  }

  /**
    * 计算欧式距离（RDD形式）
    *
    * @param vec1 RDD[(词或索引为key，对应的value)]
    * @param vec2 RDD[(词或索引为key，对应的value)]
    * @return 欧式距离相似性
    */
  def pearsonCorr(vec1: RDD[(String, Double)], vec2: RDD[(String, Double)]): Double = {

    math.sqrt(vec1.join(vec2).map(x => math.pow(x._2._1 - x._2._2, 2)).sum)
  }

}
