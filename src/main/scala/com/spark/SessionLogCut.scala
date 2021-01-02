package com.spark

import org.apache.commons.lang3.time.FastDateFormat
import scala.collection.mutable.ArrayBuffer

/**
  * 默认切割逻辑，每隔30分钟切割
  * @param sortedTrackLogs
  * @return
  */
trait SessionLogCut {

    private val dateformat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    def logCut(sortedTrackLogs:Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
        // 同一个会话buffer
        val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog]()
        // 存放所有会话buffer
        val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
        // foldLeft 从左至右遍历
        val cuttedLogsBuf = sortedTrackLogs
                  .foldLeft((initBuilder, Option.empty[TrackerLog])) { case ((builder, prelog), currlog) =>
            // 当前log时间
            val currTime: Long = dateformat.parse(currlog.getLogServerTime.toString).getTime

            if (prelog.nonEmpty) {
              // 判断下一条，是否是另一个会话session，超过30分钟就是另一个会话了
              if (currTime - dateformat.parse(
                prelog.get.getLogServerTime.toString).getTime >= 1000 * 60 * 30) {
                // 同一个会话 session buffer，存入到所有会话 session buffer里
                builder += oneCuttingSessionLogs.clone()
                // 同一个会话session清空，因为下个会话要来了
                oneCuttingSessionLogs.clear()
              }
            }

            // 同一个会话session存储在一个buffer
            oneCuttingSessionLogs += currlog

            // 第二次遍历（当前条，变成前一条）
            (builder, Some(currlog))

          }._1.result()

        // 如果不为空，说明没有超30分钟另一个会话，没有走 clear，只能在这加上
        if (oneCuttingSessionLogs.nonEmpty)
          cuttedLogsBuf += oneCuttingSessionLogs
        cuttedLogsBuf
    }

}

/**
  * 按pageview切割
  */
trait PageViewSessionLogCut extends SessionLogCut {

    override def logCut(sortedTrackLogs: Array[TrackerLog]):
                                                    ArrayBuffer[ArrayBuffer[TrackerLog]] = {
        // 同一个会话buffer
        val oneCuttingSessionLogs = new ArrayBuffer[TrackerLog]()
        // 存放所有会话buffer
        val initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
        val cuttedLogsBuf = sortedTrackLogs
                                .foldLeft(initBuilder) { case (builder,currlog) =>
              // 如果当前log是pageview的话，切割会话
              if (currlog.getLogType.toString.equals("pageview")) {
                  if(oneCuttingSessionLogs.nonEmpty){
                      // 同一个会话 session buffer，存入到所有会话 session buffer里
                      builder += oneCuttingSessionLogs.clone()
                      // 同一个会话session情况，因为下个会话要来了
                      oneCuttingSessionLogs.clear()
                  }
              }
              // 同一个会话session存储在一个buffer
              oneCuttingSessionLogs += currlog
              // 第二次遍历（当前条，变成前一条）
              builder
        }.result()
        // 如果不为空，说明没有超30分钟另一个会话，没有走 clear，只能在这加上
        if (oneCuttingSessionLogs.nonEmpty)
          cuttedLogsBuf += oneCuttingSessionLogs
        cuttedLogsBuf
    }
}