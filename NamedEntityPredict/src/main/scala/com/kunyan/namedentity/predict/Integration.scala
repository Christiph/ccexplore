package com.kunyan.namedentity.predict

import org.apache.spark.SparkContext

/**
  * Created by wangcao on 2016/12/21.
  *
  * Named entity recignition based on both dictionary and model
  */
object Integration {

  /**
    * 整合字典匹配与模型预测的两个结果
    *
    * @param sc sc: SparkContext
    * @param dict 实体词词典
    * @param text 一篇文本
    * @param path crf模型保存的路径
    * @return 3类实体词
    */
  def finalResult(sc: SparkContext, dict: Array[String], text: String, path: String): Array[String] = {

    // 1.词典匹配提取实体词
    val entityFromDict = DictMatch.dictPredict(dict, text)

    // 2.crf模型预测提取实体词
    val entityFromModel = ModelPredict.modelPredict(sc, text, path)

    // 3.整合两者的结果
    val total = entityFromDict.union(entityFromModel).distinct.sortBy(x => x)

    total
  }

}
