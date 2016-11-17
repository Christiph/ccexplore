package com.kunyan.sentencerank

import breeze.linalg.Vector
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.graphstream.graph.implementations.SingleGraph
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/11/17.
  */
object BuildGraph {

  /**
    * 导入Word2Vec模型
    *
    * @param sc SparkContext
    * @param word2vecModelPath 模型保存的路径
    * @return Word2Vec模型
    */
  def loadModel(sc: SparkContext, word2vecModelPath: String): Word2VecModel = {

    Word2VecModel.load(sc, word2vecModelPath)
  }

  /**
    * 生成文本空间向量, 文本中每个单词的词向量的加权平均。
    *
    * @param text 文本数组(分词后的一句话）
    * @param model Word2Vec模型
    * @param size Word2Vec模型中词向量的长度
    * @return 句子向量
    * @author Li
    */
  def textVectorsWithModel(text: Array[String],
                           model: Word2VecModel,
                           size: Int): Vector[Double] = {

    val wordVectors = Vector.zeros[Double](size)
    var docVectors = Vector.zeros[Double](size)
    var vector = Array[Double](size)
    var count = 0.0
    for (word <- text.indices) {

      try {
        vector = model.transform(text(word)).toArray
      }
      catch {
        case e: Exception => vector = Vector.zeros[Double](size).toArray
      }

      val tmp = Vector.apply(vector)
      wordVectors.+=(tmp)
      count += 1.0
    }

    if(count != 0) {

      // println(count)
      docVectors = wordVectors./=(count)
    }

    docVectors
  }

  /**
    * 将句向量两两遍历组合
    *
    * @param textVec
    * @return 句向量组合
    */
  def textVecPair(textVec: Map[Int, Vector[Double]]): Array[((Int, Vector[Double]), (Int, Vector[Double]))] = {

    val group = new ArrayBuffer[((Int, Vector[Double]), (Int, Vector[Double]))]

    for (i <- 0 to textVec.size -2) {

      val arr = List.range(i + 1, textVec.keys.size)

      for (j <- arr) {

        val tmp = ((i, textVec(i)), (j, textVec(j)))
        group += tmp
      }

    }

    group.toArray
  }


  /**
    * 计算余弦相似度（vector形式）
    *
    * @param vec1 词向量
    * @param vec2 词向量
    * @return 余弦相似度的值
    */
  def cosineCorr(vec1: scala.collection.immutable.Vector[Double],
                 vec2: scala.collection.immutable.Vector[Double]): Double = {

    val member = vec1.zip(vec2).map(x => x._1 * x._2).sum
    val tmp1 =  math.sqrt(vec1.map(num => {math.pow(num, 2)}).sum)
    val tmp2 =  math.sqrt(vec2.map(num => {math.pow(num, 2)}).sum)

    member / (tmp1 * tmp2)
  }

  /**
    * 计算句向量之间的两两相似性
    *
    * @param pair 句向量对
    * @return 相似度
    */
  def cosineRelation(pair: Array[((Int, Vector[Double]), (Int, Vector[Double]))]): Array[(Int, Int, Double)] ={

    val correlation = pair.map(line => {

      val vec1 = line._1._2.toArray.toVector
      val vec2 = line._2._2.toArray.toVector
      val corr = cosineCorr(vec1, vec2)

      (line._1._1, line._2._1, corr)
    })

    val correChange = correlation.map(x => (x._2, x._1, x._3))
    val finalCorrelation = correlation.union(correChange)

    finalCorrelation
  }

  /**
    * 创建图
    *
    * @param graphName 图名称
    * @param length 文本长度
    * @param vex 图的边
    * @return 图
    */
  def constructGraph(graphName: String, length: Int, vex: Array[(String, String, Double)]): SingleGraph = {

    val graph = new SingleGraph(graphName)

    //构建顶点
    List(0, length).map(_.toString).foreach(
      word => if (graph.getNode(word) == null) graph.addNode(word)
    )


    // 构建关键词图的边

    vex.sortBy(x => x._3).takeRight(1/2 * length).map(x => (x._1, x._2))
      .foreach({x =>

        if (graph.getEdge(s"${x._1} - ${x._2}") == null && graph.getEdge(s"${x._2} - ${x._1}") == null) {
          graph.addEdge(s"${x._1} - ${x._2}", x._1, x._2)

        }

      })

    graph
  }


  /**
    * 整合以上方法，输出句子与句子之间的相似度
    *
    * @param sc SparkContext
    * @param sentence 一个事件中的所有句子
    * @param path 模型保存的路径
    * @param size 词向量的大小
    * @return 句子与句子之间的相似度
    */
  def finalGraph(sc: SparkContext,
            sentence: Array[(Int,Array[String])],
            path: String,
            size: Int,
            graphName: String):  SingleGraph = {

    val model = loadModel(sc, path)
    val sentenceVec = sentence.map(x => (x._1, textVectorsWithModel(x._2, model, size))).toMap
    val pair  = textVecPair(sentenceVec)
    val edge = cosineRelation(pair).map(x => (x._1.toString, x._2.toString, x._3))
    val length = sentence.length * 1/2
    val graph = constructGraph(graphName, length, edge)

    graph
  }


}
