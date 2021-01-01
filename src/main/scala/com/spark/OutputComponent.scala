package com.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait OutputComponent {

  /**
    * 保存结果数据方法
    * @param sc
    * @param baseOutputPath
    * @param tracklogRdd
    * @param overRdd
    */
  def writeOutputData(sc: SparkContext,baseOutputPath:String
                              ,tracklogRdd: RDD[TrackerLog], overRdd: RDD[TrackerSession]) = {
      // 检查路径
      checkPath(sc,baseOutputPath)
  }

  /**
    * 检查路径
    * @param sc
    * @param trackerlogOutputPath
    * @return
    */
  protected def checkPath(sc: SparkContext, trackerlogOutputPath: String): AnyVal = {
      var fileSystem = FileSystem.get(sc.hadoopConfiguration)
      val path = new Path(trackerlogOutputPath)
      if (fileSystem.exists(path)) {
        // 递归删除
        fileSystem.delete(path, true)
      }
  }

}


object OutputComponent {

    def fromOutputFileType(fileType:String) = {
        if(fileType.equals("parquet")){
            new ParquetFileOutput
        } else {
            new TextFileOutput
        }
    }

}


/**
  * parquet文件存储
  */
class ParquetFileOutput extends OutputComponent {

  override def writeOutputData(sc: SparkContext, baseOutputPath: String
                               , tracklogRdd: RDD[TrackerLog]
                               , overRdd: RDD[TrackerSession]): Unit = {

        //先调用父类方法，去检查路径是否存在
        super.writeOutputData(sc,baseOutputPath,tracklogRdd,overRdd)

        // 保存 Trackerlog -> tracklogRdd（parquet-avro），暂时用本地路径代替
        val trackerlogOutputPath = s"${baseOutputPath}/trackerlog"
        // avro schema 写入到 parquet 中
        AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
        // 输出
        tracklogRdd.map((null, _)).saveAsNewAPIHadoopFile(
          trackerlogOutputPath // 输出路径
          , classOf[Void] // 文件key类型
          , classOf[TrackerLog] // 文件value类型
          , classOf[AvroParquetOutputFormat[TrackerLog]] // output输出类型
        )

        // 保存 TrackerSession -> overRdd（parquet-avro），暂时用本地路径代替
        val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
        AvroWriteSupport.setSchema(sc.hadoopConfiguration,TrackerSession.SCHEMA$)
        overRdd.map((null, _)).saveAsNewAPIHadoopFile(
          trackerSessionOutputPath
          , classOf[Void]
          , classOf[TrackerSession]
          , classOf[AvroParquetOutputFormat[TrackerSession]]
        )
  }

}

/**
  * textFile文件存储
  */
class TextFileOutput extends OutputComponent {

    override def writeOutputData(sc: SparkContext, baseOutputPath: String
                                 , tracklogRdd: RDD[TrackerLog]
                                 , overRdd: RDD[TrackerSession]): Unit = {
        //先调用父类方法，去检查路径是否存在
        super.writeOutputData(sc,baseOutputPath,tracklogRdd,overRdd)

        // 保存 Trackerlog -> tracklogRdd（parquet-avro），暂时用本地路径代替
        val trackerlogOutputPath = s"${baseOutputPath}/trackerlog"
        tracklogRdd.saveAsTextFile(trackerlogOutputPath)

        // 保存 TrackerSession -> overRdd（parquet-avro），暂时用本地路径代替
        val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
        overRdd.saveAsTextFile(trackerSessionOutputPath)
    }

}