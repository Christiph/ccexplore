package com.kunyan.sentencesimilarity

import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/10/21.
  */
object test {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ComputeCorrByCorr").setMaster("local")
    val sc = new SparkContext(conf)

    val vec = sc.textFile("hdfs://61.147.114.85:9000/home/warren/Corpus/2016-10-08/part-00007")
   // vec.take(2).foreach(println)

//    val urlArr = Array("http://business.sohu.com/20161003/n469552722.shtml", "http://gold.hexun.com/2016-10-01/186282418.html")
//    val new1 = vec.map(_.split("\t")).filter(x => x(0) == "http://gold.hexun.com/2016-10-01/186282418.html").map(x => x(3))
//    new1.collect().foreach(println)

//    val news1 = "继本周一欧股收创新低后，而德意志银行股价跌破10欧元股，史上首次跌至个位数。德国DAX指数下跌，而欧洲银行业股价下降。在周四宣布裁员和削减股息的计划后，德意志银行竞争对手德国商业银行股价下跌6.5%。其他欧洲银行，诸如德国联合信贷银行、巴克莱银行和法国农业信贷银行也濒临巨大损失。然而，最近几周，德意志多次为自己辩护当前并没有担忧的必要，并且有一段时间的缓冲期。路透表示，周五早上，德银联席首席执行官克莱恩对其员工表示，确保银行业并没有影响其日常业务的扭曲观点是该银行的任务。同时也重申了德意志银行拥有强大的资金支柱。"
//    val new2 = "自去年7月以来，德意志银行股价已经下跌超过六成，让投资人陷入恐慌，总市值从500亿美元下跌到160亿美元。本月早些时候，美国司法部给德意志银行开出140亿美元罚单，让德意志银行雪上加霜。如果德意志银行按此天价缴纳这笔危机前出售抵押贷款支持证券的和解金，该行的一级资本充足率将直接降至监管底线之下。数据显示，去年德意志银行已经巨亏68亿欧元，2016年二季度净利润仅有1800万欧元，同比暴跌。"
//    val stopWords = Array("。","，")
//    val kunyanConfig = new KunyanConf
//    kunyanConfig.set("222.73.57.17", 16003)
//    val seg1 = TextPreprocessing.process(news1, stopWords,  kunyanConfig).mkString(",")
//    println(seg1)

    vec.map(_.split("\t")).map(x => x(3)).take(5).foreach(println)



  }

}
