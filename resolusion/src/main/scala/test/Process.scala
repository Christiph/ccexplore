package test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2017/1/4.
  */
object Process {

  /**
    * 获取股票号与股票名称
    *
    * @param dict 股票实体的字典
    * @return Array形式的股票号与股票名称
    */
  def getStockDictArr(dict: Array[Array[String]]): Array[String] = {

    dict.flatMap(x => x)
  }

  /**
    * 获取股票号与股票名称
    *
    * @param dict 股票实体的字典
    * @return Map形式的股票号与股票名称
    */
  def getStockDictMap(dict: Array[Array[String]]): Map[String, String] = {

    dict.map(x => (x(1), x(0))).toMap
  }

  /**
    * 获取每句话中对应的股票实体
    *
    * @param text 一篇新闻
    * @param dict 股票实体的字典
    * @return （索引，股票实体，句子）
    */
  def getEntityForSentence(text: String,
                           dict: Array[Array[String]]): Array[(Int, String, String)] = {
    val dictArr = getStockDictArr(dict)

    val seg = text.trim().split("。")
        .filter(x => x.length > 1)
      .map(line => {
        val stock = new ArrayBuffer[String]

        for (entity <- dictArr) {
          if (line.contains(entity)) {
            stock += entity
          }
        }

        val len = stock.length

        if (len == 0) {
          ("0", line)
        } else {
          (stock.mkString(" "), line)
        }

      })
      .zipWithIndex
      .map(x => (x._2, x._1._1,x._1._2 ))

    seg
  }

  /**
    * 处理与优化格式
    *
    * @param addEntity 配好实体对象的所有句子
    * @param dictMap map形式的股票与股票名称
    * @return Map[股票号，句子]
    */
  def standard(addEntity: Array[(Int, String, String)],
               dictMap: Map[String, String]):  Array[(String, String)] = {

    val groupByEntity = addEntity.map(x => (x._2, x._3))
      .map(x => x._1.split(" ").map(a => (a, x._2)))
      .flatMap(x => x)
      .map(x => (dictMap.getOrElse(x._1, x._1), x._2))
      .groupBy(x => x._1)
      .map(x => (x._1, x._2.map(a => a._2).mkString("。")))
      .map(x => {
        var entity = x._1
        if (x._1 == "0") {
          entity = "There is no any stock entity in this text "
        }

        (entity, x._2)
      })

    groupByEntity.toArray
  }

  /**
    * 根据股票实体，对句子进行分组合并
    *
    * @param text 一篇文本
    * @param dict 股票实体词字典
    * @return Map[股票号，句子]
    */
  def groupByEntity(text: String,
                    dict: Array[Array[String]]): Array[(String, String)] = {

    // 提取每句话中的股票实体，并且建立句索引
    val sentenceWithIndex = getEntityForSentence(text, dict)

    val stockCount = sentenceWithIndex.count(x => x._2 != "0")
    // println("the number of entity" + stockCount)

    //提取股票号与股票名称的map
    val dictMap = getStockDictMap(dict)

    if (stockCount >= 2) {

      // 提取索引与实体的map
      val indexAndEntity = sentenceWithIndex.map(x => (x._1, x._2)).toMap

      // 提取出出现了股票实体的句子的索引
      val haveStock = sentenceWithIndex.filter(x => x._2 != "0").map(x => x._1)

      //为没有出现股票实体的句子加上前文的股票实体
      val addEntity = sentenceWithIndex.map(line => {
        val index = line._1
        var stock = line._2
        val sentence = line._3

        for (i <- haveStock.indices) {

          if (i == 0) {
            val end = haveStock(i + 1)
            val startStock = indexAndEntity(end)

            if (index < end) {
              stock = startStock
            }
          }
          else if (i > 0 && i < haveStock.length - 1) {
            val start = haveStock(i)
            val end = haveStock(i + 1)
            val startStock = indexAndEntity(start)

            if (index > start && index < end) {
              stock = startStock
            }
          }
          else if (i == haveStock.length - 1) {
            val start = haveStock(i)
            val startStock = indexAndEntity(start)

            if (index > start) {
              stock = startStock
            }
          }

        }

        (index, stock, sentence)
      })

      val finalResult = standard(addEntity, dictMap)

      finalResult
    }
    else if (stockCount == 1) {
      val finalResult = sentenceWithIndex.filter(x => x._2 != "0").map(x => x._2).mkString("")
        .split(" ").map(x => (dictMap(x), text))

      finalResult
    }
    else {
      val warn = "No entity!"
      val finalResult = Array((warn, text))

      finalResult
    }




  }

  /**
    * 预测一篇文本中提起的每支股票的情感方向
    *
    * @param sc SparkContext
    * @param path 情感分析词典的路径
    * @param text 待预测的一篇文本
    * @param stockDict 股票词典
    * @return （股票号，情感倾向，属于该股票号的文本）
    */
  def predictSentiment(sc: SparkContext,
                       path: String,
                       text: String,
                       stockDict: Array[Array[String]]): Array[(String, String, String)] = {

    // 情感分析4个词典的路径
    val dictPath = Array(path + "user.txt", path + "pos.txt", path + "neg.txt", path + "fou.txt")
    // 初始化情感分析词典
    val dictMap = PredictWithDic.init(sc, dictPath)

    //将文本根据股票实体进行句子的分类
    val textGroupByEntity = groupByEntity(text, stockDict)
    // 对每个股票实体对应的句子组合进行情感倾向的预测
    val predictSentiment = textGroupByEntity.map(x => (x._1, PredictWithDic.predict(x._2, dictMap), x._2))

    //返回预测结果
    predictSentiment
  }

  /**
    * 预测多篇文本中所提起的每支股票的情感方向
    *
    * @param sc SparkContext
    * @param path 情感分析词典的路径
    * @param text 待预测的一篇文本
    * @param stockDict 股票词典
    * @return （股票号，情感倾向，属于该股票号的文本）
    */
  def predictSentiment(sc: SparkContext,
                       path: String,
                       text: RDD[String],
                       stockDict: Array[Array[String]]): RDD[(String, String, String)] = {

    // 情感分析4个词典的路径
    val dictPath = Array(path + "user.txt", path + "pos.txt", path + "neg.txt", path + "fou.txt")
    // 初始化情感分析词典
    val dictMap = PredictWithDic.init(sc, dictPath)

    val textGroupByEntity = text.map(line => groupByEntity(line, stockDict))

    val predictSentiment = textGroupByEntity.map(line => {
      line.map(x => (x._1, PredictWithDic.predict(x._2, dictMap), x._2))
    })
      .flatMap(x => x)
      .map(x => (x._1, (x._2, x._3)))
      .groupByKey()
      .map(line => {
        val stock = line._1

        val numPos = line._2.count(x => x._1 == "pos")
        val numNeg = line._2.count(x => x._1 == "neg")
        val score = if (numPos < numNeg) {
          "neg"
        } else if (numPos > numNeg) {
          "pos"
        }  else if (numPos == numNeg && numPos != 0) {
          "neutral"
        } else {
          "null"
        }

        val wholeText = line._2.map(x => x._2).mkString("\t")

        //(stock, numPos + " " + numNeg, wholeText)
        (stock, score, wholeText)
      })

    predictSentiment
  }

}
