package com.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object SessionCutEtl {
    // 可放在数据库中的
    private val logTypeSet = Set("click","pageview")

    private val domainLabelMap = Map(
                  "www.baidu.com"->"level1"
                  ,"tieba.baidu.com"->"level2"
                  ,"jd.com"->"level3"
                  ,"youku.com"->"level4")

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sessioncut")
        //开启kryo序列化机制
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val sc: SparkContext = new SparkContext(conf)
        // 广播变量注册domainLabelMap
        val broadcastDomain: Broadcast[Map[String, String]] = sc.broadcast(domainLabelMap)

        val rowRdd: RDD[String] = sc.textFile("data/visit_log.txt")

        // 解析原始日志（生成TrackerLog表）
        val tracklogRdd: RDD[TrackerLog] = rowRdd.flatMap(RawLogParser.parse(_))
                                                 .persist(StorageLevel.MEMORY_AND_DISK)

        // 数据清洗
        val filterRdd: RDD[TrackerLog] = tracklogRdd.filter(tracklog =>
            logTypeSet.contains(tracklog.getLogType.toString)
        )

        // 按照 cookie 分组
        val userGroupRdd: RDD[(String, Iterable[TrackerLog])] =
                filterRdd.groupBy(trackLog => trackLog.getCookie.toString)

        // cookie 分组后，value 数据切割
        /**
          * (cookie1,{
          *     "session_id": "3cbb295c-3628-4eb2-8d19-0393e036aa9f",
          *     "session_server_time": "2017-09-04 12:45:01",
          *     "cookie": "cookie1",
          *     "cookie_label": "",
          *     "ip": "127.0.0.3",
          *     "landing_url": "https:\/\/tieba.baidu.com\/index.html",
          *     "pageview_count": 1,
          *     "click_count": 2,
          *     "domain": "tieba.baidu.com",
          *     "domain_label": "level2"
          *     })
          */
        //  with PageViewSessionLogCut 加上它，调用的是继承了 SessionLogCut 的 PageViewSessionLogCut
        // SessionLogCut 父类，PageViewSessionLogCut子类
        val trackerSession: RDD[(String, TrackerSession)] = userGroupRdd.flatMapValues { case iter =>
            val processor = new OneUserTrackLogProcessor(iter.toArray) with PageViewSessionLogCut
            processor.buildSessions(broadcastDomain.value)
        }

        // 给 session 的 cookie 打上 cookieLabel
        /**
          * (cookie1,固执)
          * (cookie2,有偏见)
          * (cookie3,执着)
          * (cookie4,执行力很强)
          */
        val cookieLabelRdd: RDD[String] = sc.textFile("data/cookie_label.txt")
        val cookieLabelMapRdd: RDD[(String, String)] = cookieLabelRdd.map { case line =>
            val tmps: Array[String] = line.split("\\|")
            (tmps(0), tmps(1))
        }

        val joinRdd: RDD[(String, (TrackerSession, Option[String]))] =
                                          trackerSession.leftOuterJoin(cookieLabelMapRdd)

        // session 的 cookielabel 打标签（生成TrackerSession表）
        val overRdd: RDD[TrackerSession] = joinRdd.map { case (cookie, (session, cookieLable)) =>
            if (cookieLable.nonEmpty) {
              session.setCookieLabel(cookieLable.get)
            } else {
              session.setCookieLabel("-")
            }
            session
        }.persist(StorageLevel.MEMORY_AND_DISK)

        // 保存数据
        OutputComponent.fromOutputFileType("text")
                    .writeOutputData(sc,"data/output",tracklogRdd, overRdd)

        sc.stop()
    }

}
