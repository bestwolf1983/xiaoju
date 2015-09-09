package com.xiaoju

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}

/**
 * Created by cenyuhai on 2015/9/9.
 */
object CountSmallFile {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CountSmallFile")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    var paths = fs.listStatus(new Path("/user/")).map(x=> x.getPath.toUri)
    var pathRdd = sc.parallelize(paths, paths.length)
    pathRdd.map{ uri=>

      var fs = FileSystem.get(uri, conf)

      def countFile(fs: FileSystem, path: Path): Long = {
        var sum: Long = 0
        var files = fs.listStatus(path)
        if(files != null && files.length > 0) {
          for(file <- files) {
            if(!file.getPath.getName.startsWith("_")) {
              if(file.isDirectory) {
                sum = sum + countFile(fs, file.getPath)
              } else {
                if(file.getLen < 128 * 1024 * 1024) {
                  sum = sum + 1
                }
              }
            }
          }
        }
        sum
      }
      var sum = countFile(fs, new Path(uri))
      (uri.toString, sum)
    }.coalesce(1).saveAsTextFile("/user/xiaoju/spark/CountSmallFile" + System.currentTimeMillis())
  }
}
