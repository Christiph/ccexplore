package com.kunyan.namedentity.predict

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcao on 2016/12/21.
  *
  * Named entity recognition based on dictionary
  */
object DictMatch {

  /**
    * 根据词典提取实体词
    *
    * @param dict 实体字典（包含了三类实体）
    * @param text 一个文本
    * @return 该文本中的实体词
    */
  def dictPredict(dict: Array[String], text: String): Array[String] = {

    val entity = new ArrayBuffer[String]

    for (w <- dict) {

      val word = w.split(",")(0)
      val tag = w.split(",")(1)

      if (text.contains(word)) {

        val a = tag + " " + word
        entity += a

      }
    }

    entity.toArray.distinct
  }

}
