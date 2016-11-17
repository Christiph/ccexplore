package com.kunyan.sentencesimilarity

import breeze.linalg.max
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by wangcao on 2016/10/21.
  *
  * 利用余弦相似性重构句向量，计算句向量间余弦相似性
  */
object ComputeCorrByCorr {

  /**
    * 构建句子的向量
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param wordStay 在词向量中存在的句子中的词
    * @return 句向量
    */
  def sentence2Vec(sc: SparkContext,
                   vec: RDD[(String, Array[(Double)])],
                   wordStay: Array[String]): RDD[(String,Double)] = {

    var wordCorrelation: RDD[(String,  Double)] = sc.parallelize(Array(null))

    //将每个词与词向量中的词分别两两做余弦相思性计算
    for (word <- wordStay) {

      val selectWord = vec.filter(x => x._1 == word).collect().toMap
      val wordVec = selectWord(word).toVector

      val wordCorr = vec.map(x => {

        val singleVec = x._2.toVector
        val corr = Correlation.cosineCorr(singleVec, wordVec)

        (x._1, corr)
      })

      wordCorrelation = wordCorrelation.union(wordCorr)

    }

    //取最高的相似性作为该句子在该词向量维度上的值
    val sentenceCorr = wordCorrelation.filter(x => x != null)
      .groupByKey()
      .map(x => (x._1, max(x._2)))
      .sortBy(x => x._1)

    sentenceCorr
  }


  /**
    * 运行计算句向量
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param text 句子
    * @return 句向量
    */
  def run(sc: SparkContext, vec: RDD[(String, Array[(Double)])], text: String): RDD[(String,Double)] = {

    //取出词典中的词列表
    val dicList = vec.map(_._1).collect()
    //输入文本根据逗号分隔转换成Array
    val wordArray = text.split(",")
    //词列表中有的词保留，没有的词离开（打印出离开的词）
    val wordStay = wordArray.filter(x => dicList.contains(x))
    val wordLeave = wordArray.filter(x => !wordStay.contains(x))
    println(wordLeave.length + " words are removed:" + wordLeave.mkString(","))
    //剩下词的个数
    val wordNum = wordStay.length

    //如果次数为0，则打印提示并返回null，如果大于0，则继续计算，返回句向量
    if (wordNum == 0) {

      println("the sentence:" + text + "has no word in dictionary!")

      sc.parallelize(Array(null))
    } else {

      sentence2Vec(sc, vec, wordStay)
    }

  }

  /**
    * 计算两个句子间的相似度
    *
    * @param sc SparkContext
    * @param vec word2vec训练好的词向量：RDD[(词, 词向量]
    * @param text1 句子1
    * @param text2 句子1
    * @return
    */
  def sentenceCorr(sc: SparkContext,
                   vec: RDD[(String, Array[(Double)])],
                   text1: String, text2: String):Double = {

    val score1 = run(sc, vec, text1)
    val score2 = run(sc, vec, text2)
    val sentenceCorr = Correlation.cosineCorr(score1, score2)

    sentenceCorr
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ComputeCorrByCorr").setMaster("local")
    val sc = new SparkContext(conf)

    //读入word2vec训练好的词向量
    val vec = sc.textFile("hdfs://61.147.114.85:9000/home/word2vec/model-10-100-20/2016-08-16.txt")
     // .map(x => x.replace("(", "")).map(x => x.replace(")", ""))
      .map(_.split("\t")).map(x => (x(0), x(1).split(",").map(_.toDouble))).cache()

    //文本
    val text1 = "雷士,品牌价值,稳,占,照明行业,第一,战略布局,全球,评审,委员会,揭晓,第十,届,榜单,雷士照明,以,品牌价值,连续,5年,入,选,中国最,具,价值,品牌,在,照明电器,行业,中,牢,牢,锁,住,第一,品牌,的,地位,重磅,揭晓,雷士照明,荣登,行业,第一,据,了解,本次,评估,采用,收益现值法,主要,根据,企业,最近,三年,的,盈利,水平,来,推测,品牌,未来,可能,带来,的,超额利润,再,考虑,行业特点,市场,状态,品牌,市场地位,品牌历史,等,因素,的,影响,加以,修整,过去,的,这一年,时间,对于,雷士照明,来,说,是,关键,的,一年,更,是,收获,颇丰,的,一年,2015年,雷士照明,继续,发挥,工程项目,优势,同时,紧,跟,市场需求,布局,家居照明,业务,创造,新,的,利润,增长,点,形成,工程,家居,齐,头,并举,的,营业,模式,并,通过,优化,流程,严,控,成本费用,等,改善,集团,的,盈利,水平,步,入,2016年,雷士照明,继续,传承,敢,于,突破,传统,尝试,变革与创新,的,作风,在,生产,与,采购,销售,研发,等,方面,作,积极,的,策略,部署,重塑,企业文化,力求,在,激烈,的,全球,竞争,中,赢得,发展,先机,从,2010年,的,到,今年,的,六年,时间,雷士照明,品牌价值,实现,了,跨越,式,增长,品牌价值,及,市场地位,得到,进一步,的,巩固,和,提升,商,照,家居,双,引擎,发展,助力,品牌,年轻,化,今年年,初,雷士,提出,夯实,商,照,龙头企业,的,地位"
    val text2 = "雷士,在,积极,发展商,照,市场,的,同时,加大,了,家居照明,业务,布局,今年,雷士照明,斥资,元,人民币,用,于,收购,耀,能,控股,股权,完成,收购,后,雷士照明,将,成为,中山,雷士,的,第一,位,大股东,业内,人士,分析,雷士照明,此,举,意,在,加强,雷士照明,在,家居照明,业务,的,掌控力,而,中山,雷士,则,是,雷士,旗下,家居照明,的,主力军,对,此,王冬雷,表示,雷士,家居,业务,还有,巨大,的,成长,空间,家居,业务,商,照,业务,两,手,都要,硬,目前,雷士照明,正,加大,家居销售,网络建设,加快,开店,速度,整合,产品,及,供应商,资源,通过,O2O,手段,建立,全渠道,家居,营销体系"

    //计算
    println(sentenceCorr(sc, vec,text1, text2))

    sc.stop()

 }

}
