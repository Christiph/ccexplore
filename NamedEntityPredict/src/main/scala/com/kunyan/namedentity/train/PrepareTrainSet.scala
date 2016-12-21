package com.kunyan.namedentity.train

import com.intel.ssg.bdt.nlp.{CRFModel, CRF, Token, Sequence}
import com.kunyandata.nlpsuit.util.AnsjAnalyzer
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/12/21.
  */
object PrepareTrainSet {

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
    * 为每个字添加对应的label
    *
    * @param line (word, property, label)
    * @return
    */
  def labelEachWord(line: (String, String, String)): ArrayBuffer[String] = {

    val len = line._1.length

    val arr = new ArrayBuffer[String]

    if (len == 1) {
      arr += line._1 + " " + line._2 + " " + line._3 + "_U"
    }
    else if (len == 2) {

      arr += line._1.substring(0, 1) + " " + line._2 + " " + line._3 + "_B"
      arr += line._1.substring(1) + " " + line._2 + " " + line._3 + "_E"

    } else if (len >= 3) {

      arr += line._1.substring(0,1) + " " + line._2 + " " + line._3 + "_B"

      for (i <- 1 until len-1) {
        arr += line._1.substring(i, i+1) + " " + line._2 + " " + line._3 + "_I"
      }
      arr += line._1.substring(len-1) + " " + line._2 + " " + line._3 + "_E"

    }

    arr
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PrepareTrainSet").setMaster("local")
    val sc = new SparkContext(conf)

    //读取实体词库
    val dict = sc.textFile("D:\\ccccc\\NamedEntityPredict\\target\\data\\totalDict_final")
      .map(_.split(",")).filter(x => x.length == 2).map(x => (x(0), x(1))).distinct()
      .collect().toMap

    //给训练数据分词
    val data = sc.textFile("D:\\Documents\\cc\\学习资料\\实体识别调研\\实验数据\\hbase_news\\00007\\part-00000")
      .map(_.split("。"))
      .flatMap(x => x)
      .map(x => segWord(x))

    //给训练数据的每个词打上标签
    val proData = data.map(line => {

      val sentence = line.map(x => x.split("/"))
        .filter(x => x.length ==2)
        .map(x => (x(0), x(1), dict.getOrElse(x(0), "O")))
        .map(x => labelEachWord(x))
        .flatMap(x => x)

      sentence  // (word, property, label)
    })

    val train = proData.map(line => {

      Sequence(line.map(x => x.trim())
        .filter(token => token.split(" ").length == 3)
        .map(token => {
          val tags = token.split(" ")
          Token.put(tags.last, tags.dropRight(1))
      }))

    })

    val template = Array("U00:%x[-1,0]", "U01:%x[0,0]", "U02:%x[1,0]", "U03:%x[2,0]","U04:%x[-2,0]",
      "U05:%x[-1,0]/%x[0,0]","U06:%x[0,0]/%x[1,0]","U07:%x[-2,1]","U08:%x[-1,1]","U09:%x[0,1]",
      "U10:%x[1,1]","U11:%x[2,1]","U12:%x[-2,1]/%x[-1,1]","U13:%x[-1,1]/%x[0,1]","U14:%x[0,1]/%x[1,1]",
      "U15:%x[1,1]/%x[2,1]","B")

    val model = CRF.train(template, train)
    sc.parallelize(CRFModel.saveArray(model)).saveAsTextFile("D:\\ccccc\\NamedEntityPredict\\target\\model\\crfmodel1\\")

    sc.stop()
  }



}
