package com.spark

object RawLogParser {

  /**
    * 原始日志转成TrackLog对象
    * @param line
    * @return
    */
    def parse(line:String):Option[TrackerLog] = {
        if(line.startsWith("#")) None
        else {
            val fields = line.split("\\|")
            val trackerLog = new TrackerLog()
            trackerLog.setLogType(fields(0))
            trackerLog.setLogServerTime(fields(1))
            trackerLog.setCookie(fields(2))
            trackerLog.setIp(fields(3))
            trackerLog.setUrl(fields(4))
            Some(trackerLog)
        }
    }


}
