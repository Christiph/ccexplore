package com.kunyan.newsvisit

import java.util.{Properties, Date}
import com.kunyandata.nlpsuit.util.JsonConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcao on 2017/1/13.
  */
object NewsVisitComputing {

  /**
    * 读取新闻标题,url,关联的股票号,情感值
    *
    * @param jsonConfig 配置文件
    * @param sqlContext sql实例
    * @param partition 自定义分区数
    * @return （url, 时间戳，关联的股票号）
    * @note rowNum: 18
    */
  def readNewsData(jsonConfig: JsonConfig,
                   sqlContext: SQLContext,
                   partition: Int): RDD[(String, (String, String, Int))] = {

    val currentTimestamp = new Date().getTime

    val propNews = new Properties()
    propNews.setProperty("user", jsonConfig.getValue("computeRelation", "userNews"))
    propNews.setProperty("password", jsonConfig.getValue("computeRelation", "passwordNews"))
    propNews.setProperty("driver", "com.mysql.jdbc.Driver")

    sqlContext.read
      .jdbc(jsonConfig.getValue("computeRelation", "jdbcUrlNews"),
        "news_info", "news_time", 1L, currentTimestamp, 4, propNews)
      .registerTempTable("tempTableNews")

    val news = sqlContext.sql(s"select * from tempTableNews where type = 0")

    news.map(row => {
      ( row.getString(5), (row.getString(4), row.getString(9), row.getInt(12)))
    }).repartition(partition).filter(x => x._2._2.length > 0)

  }

  def readTeleData(sc:SparkContext, path:String): RDD[(String, (String, Int))] = {

    val data = sc.textFile(path)
      .filter(x => x != "")
      .map(_.split(","))
      .filter(_.length == 2)
      .filter(x => x(0).length > 10)
      .map(x => x(0).substring(0,10) + " " + x(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x =>(x(0), x(1).toInt))
      .reduceByKey(_ + _)
      .map(x => (x._1.split(" "), x._2))
      .map(x => (x._1(1), (x._1(0), x._2)))

    data
  }

  def joinTwoTable(newsData:RDD[(String, (String, String, Int))],
                   teleData:RDD[(String, (String, Int))]) = {

    //(date, url, title, stock, sentiment, visit)
    val join = teleData.join(newsData)
      .map(x => (x._2._1._1, (x._1, x._2._2._1, x._2._2._3, x._2._2._2, x._2._1._2)))
      .groupByKey()
      .map(line => {
        val date = line._1
        val top10 = line._2.toArray.sortWith(_._5 > _._5).take(10)

        (date, top10)
      })
      .sortByKey()
      .map(x => x._2.map(a => (x._1, a)))
      .flatMap(x => x)

    val lineByStock = join.map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5))
      .map(x => x._5.split(",").map(a => x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4 + "\t" + x._6 + "\t" + a))
      .flatMap(x => x)

    lineByStock
  }

}
