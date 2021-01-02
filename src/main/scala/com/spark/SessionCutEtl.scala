package com.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
会话切割项目的程序入口
方案一：jars打到submit里面，所以项目包用 spark-sessioncut-1.0-SNAPSHOT.jar
方案二：submit没有打入jars，所以要 spark-sessioncut-1.0-SNAPSHOT-jar-with-dependencies.jar

./spark-submit --class com.spark.SessionCutEtl \
--master spark://hadoop1:7077 \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--total-executor-cores 2 \
--jars /opt/module/spark/spark-course/parquet-avro-1.8.1.jar \
--conf spark.SessionCutEtl.visitLogsInputPath=hdfs://hadoop1:9000/user/sparksessioncut/visit_log.txt \
--conf spark.SessionCutEtl.cookieLogsLabelPath=hdfs://hadoop1:9000/user/sparksessioncut/cookie_label.txt \
--conf spark.SessionCutEtl.outputPath=hdfs://hadoop1:9000/user/sparksessioncut/output \
/opt/module/spark/spark-course/spark-sessioncut-1.0-SNAPSHOT.jar text

或者用：
./spark-submit --class com.spark.SessionCutEtl \
--master spark://hadoop1:7077 \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--total-executor-cores 2 \
--conf spark.SessionCutEtl.visitLogsInputPath=hdfs://hadoop1:9000/user/sparksessioncut/visit_log.txt \
--conf spark.SessionCutEtl.cookieLogsLabelPath=hdfs://hadoop1:9000/user/sparksessioncut/cookie_label.txt \
--conf spark.SessionCutEtl.outputPath=hdfs://hadoop1:9000/user/sparksessioncut/output \
/opt/module/spark/spark-course/spark-sessioncut-1.0-SNAPSHOT-jar-with-dependencies.jar parquet
  */
object SessionCutEtl {
    // 可放在数据库中的
    private val logTypeSet = Set("click","pageview")

    private val domainLabelMap = Map(
                  "www.baidu.com"->"level1"
                  ,"tieba.baidu.com"->"level2"
                  ,"jd.com"->"level3"
                  ,"youku.com"->"level4")

    private val logger = LoggerFactory.getLogger("SessionCutEtl")

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("sessioncut")
        if(!conf.contains("spark.master")){
            conf.setMaster("local[*]")
        }
        //开启kryo序列化机制
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

        //通过配置拿到输入输出路径
        val visitLogsPath = conf.get("spark.SessionCutEtl.visitLogsInputPath"
                                                        ,"data/visit_log.txt")
        val cookieLogsLabelPath = conf.get("spark.SessionCutEtl.cookieLogsLabelPath"
                                                        ,"data/cookie_label.txt")
        val outputPath = conf.get("spark.SessionCutEtl.outputPath"
                                                        ,"data/output")

        val outputFileType = "text"

        val sc: SparkContext = new SparkContext(conf)
        // 广播变量注册domainLabelMap
        val broadcastDomain: Broadcast[Map[String, String]] = sc.broadcast(domainLabelMap)

        val rowRdd: RDD[String] = sc.textFile(visitLogsPath)

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
          * flatMapValues 前
          * (cookie2,
          *     CompactBuffer(
          *         {"log_type": "pageview", "log_server_time": "2017-09-04 12:00:01"
          *                     , "cookie": "cookie2", "ip": "127.0.0.4", "url": "https:\/\/www.baidu.com"},
          *         {"log_type": "pageview", "log_server_time": "2017-09-04 12:00:02"
          *                     , "cookie": "cookie2", "ip": "127.0.0.4", "url": "http:\/\/news.baidu.com"},
          *         {"log_type": "click", "log_server_time": "2017-09-04 12:00:03"
          *                     , "cookie": "cookie2", "ip": "127.0.0.4", "url": "http:\/\/news.baidu.com"}
          *     )
          * )
          *
          * flatMapValues 后
          * {"log_type": "pageview", "log_server_time": "2017-09-04 12:00:01", "cookie": "cookie2"
          *                                         , "ip": "127.0.0.4", "url": "https:\/\/www.baidu.com"}
          * {"log_type": "pageview", "log_server_time": "2017-09-04 12:00:00", "cookie": "cookie1"
          *                                         , "ip": "127.0.0.3", "url": "https:\/\/www.baidu.com"}
          */
        // with PageViewSessionLogCut 加上它，调用的是继承了 SessionLogCut 的 PageViewSessionLogCut
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
        val cookieLabelRdd: RDD[String] = sc.textFile(cookieLogsLabelPath)
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

        logger.info(s"=============overRdd==============${overRdd.count()}")

        // 保存数据
        OutputComponent.fromOutputFileType(outputFileType)
                    .writeOutputData(sc,outputPath,tracklogRdd, overRdd)

        sc.stop()
    }

}
