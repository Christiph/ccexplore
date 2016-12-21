package com.kunyan.namedentity.predict

import com.intel.ssg.bdt.nlp.{CRFModel, Sequence, Token}
import com.kunyandata.nlpsuit.util.AnsjAnalyzer
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/12/21.
  *
  * Named entity recognition based on model
  */
object ModelPredict {

  /**
    * 用ansj给文本分词并带上词性
    *
    * @param text 一篇文本
    * @return  分词后的文本
    */
  def segWord(text: String): Array[String] = {

    val pre = text.trim()
    val segSearchWord = AnsjAnalyzer.cutWithTag(pre).mkString("\t")
      .split("\t")
      .filter(x => x != "始##始")

    segSearchWord
  }

  /**
    * 将词语拆分成单字
    *
    * @param segWord 分词后的句子
    * @return 分成单字的文本
    */
  def changeToSingeWord(segWord: Array[String]): Array[String] = {

    val arr = new ArrayBuffer[String]

    segWord.map(_.split("/"))
        .filter(x => x.length == 2)
        .foreach(line => {

          val w = line(0).toArray.map(_.toString)
          val tag = line(1)

          for (i <- w.indices) {
            arr += w(i) + " " + tag
          }

        })

    arr.toArray
  }

  /**
    * 处理文本为模型需要的格式
    *
    * @param news 文本，RDD每个元素为1个句子
    * @return 模型需要的文本格式
    */
  def standardFormat(news: Array[String]): Array[Sequence] = {

    val data = news.map(x => segWord(x))
      .map(x => changeToSingeWord(x))
      .map(x => {

        Sequence(x.map(token => {
          val tags:Array[String] = token.split(" ")
          Token.put(tags)
        }))

      })

    data
  }

  /**
    * 从模型的预测结果中提取实体词
    *
    * @param result 模型的结果
    * @return 实体词
    */
  def getEntityFromPredict(result: Array[Sequence]): Array[String] = {

    result.map(x => x.toArray)
      .map(x => x.map(a => a.tags.take(1).mkString("") + " " + a.label))
      .map(x => {

        val target = x.filter(x => !x.contains("O_"))
          .map(x => {

            var line = x

            if (x.endsWith("_B")) {
              line = x.split(" ")(1) + " " + x.split(" ")(0)
            }
            else if (x.endsWith("_I")) {
              line = x.split(" ")(0)
            }
            else if (x.endsWith("_E")) {
              line = x + "\t"
            }

            line
          })

        target.mkString("")
      })
      .map(_.split("\t"))
      .filter(x => x != "")
      .map(line => {

        line.map(_.split(" "))
          .filter(x => x.length == 3)
          .map(a => a(0).split("_")(0) + " " + a(1))
          .mkString("\t")

      })
      .mkString("\t")
      .split("\t")
      .filter(x => x!= "")

  }

  /**
    * 导入模型进行预测
    *
    * @param sc SparkContext
    * @param path 模型保存的路径
    * @return 预测并提取的实体词
    */
  def modelPredict(sc: SparkContext, text: String, path: String): Array[String] = {

    //将一篇文本转换成由句子组成的Array
    val sentenceSplit = text.split("。")

    //将文本转换成模型能识别的格式
    val preparedData = standardFormat(sentenceSplit)

    //导入模型
    val model = CRFModel.loadArray(sc.textFile(path).collect())

    //预测
    val result = model.predict(preparedData)

    //将预测结果转换成需要的格式
    val entity = getEntityFromPredict(result)

    entity
  }

}
