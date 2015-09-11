package com.xiaoju

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}

/**
 * Created by cenyuhai on 2015/9/9.
 */
object CountSmallFile {
  def main(args: Array[String]): Unit = {
    var scanPath: String = args(0)
    val sparkConf = new SparkConf().setAppName("CountSmallFile")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    //val hadoopConf = new SparkHadoopConfiguration(conf)
    val fs = FileSystem.get(conf)
    var paths = fs.listStatus(new Path(scanPath)).map(x=> x.getPath.toUri)
    paths.foreach(uri=> println(uri.toString))
    var pathRdd = sc.parallelize(paths, paths.length)
    pathRdd.map{ uri=>
      var fs = FileSystem.get(uri, new Configuration())
      def countFile(fs: FileSystem, path: Path): Long = {
        var sum: Long = 0
        var files = fs.listStatus(path)
        println("ls " + path.getName)
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
      println("start to count:" + uri.toString)
      var sum = countFile(fs, new Path(uri))
      (uri.toString, sum)
    }.reduceByKey(_ + _).repartition(1).sortBy(_._2).saveAsTextFile("/user/xiaoju/spark/CountSmallFile" + System.currentTimeMillis())
  }
}
