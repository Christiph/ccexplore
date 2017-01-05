package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by wangcao on 2017/1/3.
  */
object Test1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test1").setMaster("local")
    val sc = new SparkContext(conf)

    // 获取股票实体词字典
    val stockDict = sc.textFile("D:\\ccccc\\resolusion\\dict\\stockDict\\stock_words.words")
        .map(_.split("\t")(1)).map(_.split(",")).collect()

    // 定义情感分析词典的路径
    val path = "D:\\ccccc\\resolusion\\dict\\sentimentDict\\"

    // 待预测的一篇文本
   //val text = "在近一个月新股融资规模不超10亿元之后，即将启动申购的一批新股中，则隔周被安排了两只融资规模较大的新股，这意味着在目前尚能预见的情况下，A股市场就将面临80亿元左右的分流压力。在新股供给量和融资规模加大的同时，次新股的行情却在走下坡路，行情不佳的背后，则是资金在抱团加速撤离，营业部的卖出前三名中多有次新股的身影。A股恐遭遇80亿左右分流今年下半年以来，在新股发行节奏加快的同时，也有一些融资体量大的新股被间隔性的安排发行。而在即将启动申购的一批新股中，则有两只融资规模较大的新股将面世。相较前两个月，最近两个月的新股发行节奏明显加快。数据显示，6月份和7月份启动申购的新股分别有14只和16只，而到8月份则大幅增加至27只，9月份则有23只新股启动了申购程序。以融资规模来看，在近一个月，启动申购的新股募集资金均维持在10亿元以内，但即将开启申购程序的部分新股融资规模明显放大，预计总融资规模近80亿元。公开资料显示，目前5只新股等待启动申购程序，其中，9月的最后一周有4只新股待申购，分别为博创科技、恒康家居、顾家家居、汇顶科技;另外，原本9月23日启动申购的杭州银行推迟至10月14日。在上述5只新股中，作为银行股的杭州银行预计融资规模最大，达到37.67亿元，成为今年下半年以来第五大融资体量的新股;顾家家居则以20.34亿元的预计融资金额位列今年下半年募资规模第七位，其余3只新股的预计募集资金则在2.43亿元-9.25亿元区间。这意味着在目前尚能预见的情况下，A股市场就可能将遭遇78.42亿元的分流。在刚刚过去的一周，已有9只新股启动申购累计募集资金44.34亿元，若当时杭州银行按照原计划进行申购，则拟融资规模累计为82.01亿元。事实上，杭州银行除了延期申购以外，首发募资规模由原定的6亿股减半至约2.62亿股。在业内看来，银行股融资规模比较大会吸引很多的资金来打新，从而可能影响A股市场的稳定。资金抱团撤离次新炒作在新股供给量和融资规模加大的同时，新股以及次新股的行情却在走下坡路。尤其是8月下旬开始，次新股的表现不尽人意，呈现出震荡下行的态势。对应的则是背后资金的抱团撤离。从已上市新股开板前不含上市首日表现的平均涨停板数量来看，6月份和7月份平均涨停板数分别为17.69个、17.08个，8月份已开板和未开板的涨停板累计平均数为12.83个。而9月份已开板的新股中，涨停板最多的为11个，其中三角轮胎4个涨停板之后就开板。开板前的平均涨停板数呈下滑态势。在前期被火热炒作的次新股迎来退潮期。wind概念板块中的次新股指数近期表现不佳，最近的9月23日，以2.5%的跌幅领跌;近5个交易日和20个交易日的走势不尽人意，分别以1.85%、12.23%的跌幅领跌，最近60个交易日也开始处于跌势。在次新股行情不佳的背后，则是资金在抱团加速撤离。以8月25日至9月25日期间的资金情况来看，营业部中卖出金额最多的华泰证券深圳益田路荣超商务中心证券营业部卖出的前三名均为次新股，依次为江阴银行、达志科技、优博讯。安德利、欧普照明、上海电影等也多次出现在卖出金额较多的营业部前三名卖出名单中。有业内观点指出，新股发行节奏加快以及大盘新股的频繁来袭，将可能会市场存量资金带来一定的分流压力，甚至加剧股市的调整压力。近期也有多数观点表示，炒新退潮可能与当前整体市场行情有关。“炒新和市场互为因果，市场退潮前新股先退。”一位沪上券商高管表示。“新股的下跌进一步带动指数下跌，指数下跌打击炒新热情。”上述券商高管表示，市场其实一直在调整阶段，处于弱市，在经济乏善可陈的背景下，大部分股票被市场所遗弃。"

    // 待预测的多篇文本
    val text = sc.parallelize(Array("披露的招股书，IPO发行规模不超过12亿股，发行后总股本不超过66.04亿股。不过，从本年内上市的江苏银行、江阴银行、杭州银行、无锡银行等首次公开发行规模均遭受折腰的境遇来看，上海银行本次IPO规模被削减是大概率事件。住房按揭占半壁江山 金融市场业务迅猛上海银行是城商行队列中的“二哥”，资产规模和盈利水平仅次于北京银行。此次A股闯关成功，即将使得A股城商行阵营进一步扩容和实力提升。2007年7月，南京银行、宁波银行先后登陆A股市场，9月北京银行也随之挂牌上市。城商行中历史发展较久、实力强劲的上海银行也按耐不住，2008年董事会通过决议谋划上市，但遗憾的是，此后的9年再无银行能够问鼎A股市场。自此，在城商行梯队，在资产规模和盈利能力排名靠前的几家城商行“弟兄”被鲜明的划为两个行列：上市银行和非上市银行。排名“老二”的上海银行与北京银行、南京银行、宁波银行之间隔上了一道A股的闸门。9年间，中国银行业从中高速增长黄金时期跌落，上海银行自身反倒保持着良好的业绩表现。2015年年报显示，截止到2015年末，上海银行资产规模14491.40亿元，同比增长22.04%，净利润130.43亿元，同比增长14.42%，营业收入331.59亿元，同比增长18.01%，手续费及佣金收入55.08亿元，增幅39.55%，零售贷款占贷款比重16.12%。贷款规模5365.08亿元，同比增长10.73%；存款余额7926.80亿元，同比增长9.36%。当前，信用风险上升、银行资产投放重点纷纷转向住房按揭贷款，上海银行便是典型。年报显示，2014年、2015年该行住房按揭贷款分别为450.11亿元、473.82亿元，占据了个人贷款的半壁江山，比重分别为54.79%、65.74%。对公方面，房地产贷款更是雄踞上海银行公司类贷款霸主地位，2015年房地产贷款余额746.46亿元，位居贷款投放行业第一，占全行贷款总额的13.91%，不良贷款率仅0.24%。对此，上海银行表示，近年来，上海市以及上海银行各异地分支机构所在城市房地产行业发展较快，因此其对房地产企业贷款的比重一直较大。其招股说明书中坦言，“本行面临与我国房地产行业相关的风险敞口，尤其是个人住房贷款和以房地产作为抵押的贷款。”值得关注的是，从财报来看，上海银行金融市场投资业务增长迅猛。截止到2015年末，该行投资总额5903.27亿元，同比增加2064.87亿元，增幅53.79%。其中，以公允价值计量且其变动计入当期损益的金融资产同比减少33.77亿元，可供出售金融资产、持有到期金融资产和应收账款类投资同比增加779.04亿元、439.49亿元、880.11亿元。2015年该行资金业务规模达7326.41亿元，同比增加1768.54亿元。作为城商行队伍的排头兵，城商行早已在综合化经营布好棋局。上海银行控股家村镇银行，并持有光大银行、申联国际、中国银联、城商行清算中心四家金融机构可控出售金融资产。在子公司布局方面，上海银行全资控股上海银行（香港）有限公司，并控股上银基金有限公司。从资产质量而言，相较于当前银行业不良资产继续爆发的行情，上海银行近三年资产质量表现良好。上海银行2015年不良贷款余额63.70亿元，同比增加16.39亿",
    "元，增幅34.65%；不良贷款率1.19%，同比提高0.21个百分点，计提拨备78.34亿元，同比增加25.46亿元，拨备覆盖率237.70%，同比下降22.85个百分点，拨贷比2.82%。2013年、2014年不良贷款率均不足1%，分别为0.82%、0.98%。从资本充足率指标来看，上海银行2015年核心一级资本充足率10.32%，一级资本充足率12.65%。股权分散城商行股改的先行者根据上海银行招股书对外公布的股权情况显示，该行共有股东40109名，其中自然人股东38850名，合计持有919,225,876股股份，占发行人股份总数的19.54%；机构股东1259名，合计持有3,784,774,124股股份，占发行人股份总数的80.46%。该行的自然人股东数量较多，远高于法人股东。招股书显示，持有上海银行股份总数5%以上的股东共有四家：联和投资公司、西班牙桑坦德银行、上港集团和建银投资公司，持股比例分别为15.36%、7.2%、7.2%、5.48%。也就是说，上海银行不存在控股股东及实际控制人，股份过于分散恐为其上市后发展埋下隐忧。上海银行前十大股东还有中船国际贸易有限公司、TCL集团股份有限公司、上海商业银行有限公司、上海市黄浦区国有资产总公司、上海汇鑫投资经营有限公司、中信国安有限公司。前十大股权持股比例合计52.67%。从股本结构来看，上海银行是一家国有股占主导的城商行。截止到2015年末，该行国有股持股比例达55.51%，外资股占比10.20%，其他法人股占比14.75%，自然人股19.54%。与多数城商行面临的普遍问题在于，股权分散也表现在上海银行中。年报显示，截止到2015年7月31日，该行共有206家国有股东，合计持有股份302.65亿股。梳理上海银行发展路径不难发现，股权分散是历史包袱所致。1996年1月30日，上海银行在原上海市98家城市信用合作社和上海市城市信用合作社联社基础上组建而成。当时设立时的名称为“上海城市合作银行”，注册资本为15.68亿元，是我国成立时间最早的城市商业银行之一。 1998年7月，上海银行在城商行股份制改造也走在前列。由“上海城市合作银行”变更为“上海银行股份有限公司”获批。1999年和2001年，上海银行进行增资扩股，引入了上海商业银行等境外金融机构参与认购该行股份，成为我国第一家引进境外投资者的城市商业银行。发展至今，上海银行共经历了五轮增资扩股，从一家地方财政局占有控股话语权的城商行，逐步走向多元化股东背景的公司治理结构。"))

    // 预测一篇文本中的每支股票的情感倾向
    val predict = Process.predictSentiment(sc, path, text, stockDict)

    predict.foreach(println)

  }
}