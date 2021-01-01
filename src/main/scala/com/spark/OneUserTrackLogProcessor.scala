package com.spark

import java.net.URL
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
  * 单个user（cookie）下面的所有tracklogs进行session切割
  *
  * 数据:
  * (cookie1,
  *   CompactBuffer(
  *     {"log_type": "pageview", "log_server_time": "2017-09-04 12:00:00",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/www.baidu.com"},
  *     {"log_type": "click", "log_server_time": "2017-09-04 12:00:02",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/www.baidu.com"},
  *     {"log_type": "click", "log_server_time": "2017-09-04 12:00:04",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/www.baidu.com"},
  *     {"log_type": "pageview", "log_server_time": "2017-09-04 12:45:01",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/tieba.baidu.com\/index.html"},
  *     {"log_type": "click", "log_server_time": "2017-09-04 12:45:02",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/tieba.baidu.com\/index.html"},
  *     {"log_type": "click", "log_server_time": "2017-09-04 12:45:03",
  *                 "cookie": "cookie1", "ip": "127.0.0.3", "url": "https:\/\/tieba.baidu.com\/index.html"}
  *     )
  * )
  *
  * 处理结构：
  * var session_id: CharSequence = null
  * var session_server_time: CharSequence = null
  * var cookie: CharSequence = null
  * var cookie_label: CharSequence = null
  * var ip: CharSequence = null
  * var landing_url: CharSequence = null
  * var pageview_count: Int = 0
  * var click_count: Int = 0
  * var domain: CharSequence = null
  * var domain_label: CharSequence = null
  */
class OneUserTrackLogProcessor(trackLogs:Array[TrackerLog]) extends SessionLogCut {

    // 日志按照时间升序排列，两两日志升序排，时间超过30分钟，算另一个会话session了
    private val sortedTrackLogs = trackLogs.sortBy(_.getLogServerTime.toString)

    def buildSessions(domainLabelMap:Map[String,String]):ArrayBuffer[TrackerSession]= {

        // 会话日志,按时间切割
        val cuttedLogsBuf = logCut(sortedTrackLogs)

        // 生成会话对象
        mkSession(cuttedLogsBuf,domainLabelMap)
    }

    /**
      * 生成会话
      * @param cuttedLogsBuf
      * @param domainLabelMap
      * @return
      */
    private def mkSession(cuttedLogsBuf: ArrayBuffer[ArrayBuffer[TrackerLog]]
                          ,domainLabelMap:Map[String,String]): ArrayBuffer[TrackerSession] = {
        // 生成TrackerSession会话，每一个都是一个会话buff，往TrackerSession里面装
        cuttedLogsBuf.map { case sessionLogs =>
            val session: TrackerSession = new TrackerSession()
            session.setSessionId(UUID.randomUUID().toString)
            // 第一条日志时间
            session.setSessionServerTime(sessionLogs.head.getLogServerTime)
            // cookie都一样，用第一条日志的
            session.setCookie(sessionLogs.head.getCookie)
            session.setCookieLabel("")
            session.setIp(sessionLogs.head.getIp)
            // landingurl 是 pageview 第一条日志的 url,不是 click 的 url
            val pageviewLogs = sessionLogs.filter(_.getLogType.toString.equals("pageview"))
            if (pageviewLogs.length == 0) {
                session.setLandingUrl("-")
                session.setDomain("-")
            } else {
                session.setLandingUrl(pageviewLogs.head.getUrl)
                val url = new URL(pageviewLogs.head.getUrl.toString)
                session.setDomain(url.getHost)
            }
            session.setPageviewCount(pageviewLogs.length)
            val clicklogs = sessionLogs.filter(_.getLogType.toString.equals("click"))
            session.setClickCount(clicklogs.length)
            val domainLabel: String = domainLabelMap.getOrElse(session.getDomain.toString, "-")
            session.setDomainLabel(domainLabel)
            session
        }
    }

}
