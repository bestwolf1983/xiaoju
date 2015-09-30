package com.xiaoju

import java.net.InetAddress
import java.sql._
import java.util
import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{ClientCache, JobID, ResourceMgrDelegate}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.Array


/**
 * Created by 岑玉海 on 15-4-14.
 */
object RMLogStreaming {

  private var submit_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*USER=([a-zA-Z0-9]*)[ \t]*IP=([.0-9]*).*OPERATION=Submit Application Request.*RESULT=([a-zA-Z0-9]*)[ \t]*APPID=([a-zA-Z0-9_]*)")
  private var register_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*USER=([a-zA-Z0-9]*)[ \t]*IP=([.0-9]*).*OPERATION=Register App Master.*RESULT=([a-zA-Z0-9]*)[ \t]*APPID=([a-zA-Z0-9_]*).*")
  private var queue_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue.*Application[ \t]*([a-zA-Z0-9_]*).*activated in queue:[ \t]*([a-zA-Z0-9]*).*")
  private var release_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*USER=([a-zA-Z0-9]*)[ \t]*OPERATION=AM Released Container.*RESULT=([a-zA-Z0-9]*)[ \t]*APPID=([a-zA-Z0-9_]*).*")
  private var finish_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*USER=([a-zA-Z0-9]*)[ \t].*OPERATION=Application Finished.*RESULT=([a-zA-Z0-9]*)[ \t]*APPID=([a-zA-Z0-9_]*).*")
  private var summary_pattern: Pattern = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*ApplicationSummary.*appId=([a-zA-Z0-9_]*).*")
  private var pp: Pattern = Pattern.compile("[\t|\r|\n]*")

  var checkpointDirectory = "/data/xiaoju/spark/checkpoint"

  var table = "job_audit"
  var driver: String = "com.mysql.jdbc.Driver"
  var url: String = "jdbc:mysql://bigdata-arch-hdp801.qq:3306/monitor?autoReconnect=true&failOverReadOnly=false"
  var user: String = "root"
  var pass: String = "123456"

  var topic = "resourcemanager"
  var brokerList = "hdp71.qq:9099,hdp72.qq:9092,hdp73.qq:9092"
  var zkList = "hdp71.qq:2181,hdp72.qq:2181,hdp73.qq:2181"
  var groupId = "spark-rmlog"

  var rmAddress = "hdp800.qq:18040"
  var hisServerAddress = "hdp800.qq:10020"

  def createStreamingContext(time: String): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("RMLogStreaming")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.shuffle.service.enabled", "false")
    val ssc = new StreamingContext(sparkConf, new Duration(time.toInt))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId
    )

    val topicsSet = Set(topic)
    val topicsMap = Map[String, Int](topic -> 1)

    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val messages = KafkaUtils.createStream(ssc, zkList, groupId, topicsMap)
    messages.map(_._2).map(fetchMessageFromLog).filter(x=> x._1 != null).foreachRDD(rdd => {
      try {
        if (!rdd.partitions.isEmpty) {
          val client = createYarnClient()
          val jobs = getAllJobs(client).map(new AppStatus(_))
          rdd.foreachPartition(partitionOfRecords => handlePartition(partitionOfRecords, jobs))
          closeYarnClient(client)
        }
      }catch {
        case e: Exception => e.printStackTrace()
      }

    })
    // ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def closeYarnClient(client: ResourceMgrDelegate) = {
    try {
      if (client != null) client.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def handlePartition(partitionOfRecords: Iterator[(String, util.HashMap[String,String])], jobs: Array[AppStatus]) = {
    var conn: Connection = null
    var statement: Statement = null
    var partClient: ResourceMgrDelegate = null
    try {
      partClient = createYarnClient()
      val clientCache = new ClientCache(getConf(), partClient)
      conn = openConnection()
      statement = conn.createStatement()

      partitionOfRecords.foreach(item => {
        try {
          val jobStatus = getJobStatus(jobs, item._1)
          var jobInfo = item._2
          var flg = jobInfo.get("flg")

          if(jobStatus != null || "finish" == flg ||  "summary" == flg) {
            jobInfo = judgeFromJobStatus(item._1, clientCache, jobStatus, jobInfo)
            val sql = createSql(item._1, jobInfo)
            if(sql != "") statement.execute(sql)
          }
        }catch {
          case _ =>
        }
      })
    }catch {
      case e: Exception =>
    } finally {
      closeConnection(statement, conn, partClient)
    }
  }


  def closeConnection(statement: Statement, conn: Connection, client: ResourceMgrDelegate) = {
    try {
      if (statement != null) statement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    try {
      if (conn != null) conn.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    try {
      if (client != null) client.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def createSql(jobId: String, jobInfo: util.HashMap[String, String]): String = {
    var sql = new StringBuffer("")
    val flg = jobInfo.get("flg")

    try {
      if ("submit" == flg) {
        val jobName: String = jobInfo.get("jobname")
        val loggerTime: String = jobInfo.get("time")
        val ugi: String = jobInfo.get("ugi")
        val host: String = jobInfo.get("host")

        sql.append("INSERT INTO " + table + " (progress,ugi,logger_time,host,jobid,jobname) VALUES ('%s','%s','%s','%s','%s',\"%s\") ".format("SUBMIT", ugi, loggerTime, host, jobId, jobName))
        sql append ("ON DUPLICATE KEY UPDATE logger_time='%s', host='%s',jobname=\"%s\" ".format(loggerTime, host, jobName))

      }
      else if ("register" == flg) {
        val mapNum: Int = jobInfo.get("mapnum").toInt
        val redNum: Int = jobInfo.get("rednum").toInt
        val jobConf: String = jobInfo.get("jobconf")
        val state: String = jobInfo.get("state")

        sql.append("INSERT INTO " + table + " (state,progress,ugi,jobid,mapnum,rednum) VALUES ('%s','%s','%s','%s',%d,%d) ".format(state, "REGISTER", jobInfo.get("ugi"), jobId, mapNum, redNum))
        sql.append("ON DUPLICATE KEY UPDATE state='%s',progress='%s',jobconf='%s',mapnum=%d,rednum=%d ".format(state, "REGISTER", jobConf, mapNum, redNum))
      }
      else if ("release" == flg) {
        var progress: String = jobInfo.get("mapprogress")
        if (progress == null) return ""

        progress = jobInfo.get("redprogress")
        if (progress == null) return ""

        val mapProgress: Double = jobInfo.get("mapprogress").toDouble
        val redProgress: Double = jobInfo.get("redprogress").toDouble

        sql.append("INSERT INTO " + table + " (ugi,mapprogress,redprogress,jobid) VALUES ('%s',%.3f,%.3f,'%s') ".format(jobInfo.get("ugi"), mapProgress, redProgress, jobId))
        sql.append("ON DUPLICATE KEY UPDATE mapprogress=%.5f,redprogress=%.5f ".format(mapProgress, redProgress))
      }
      else if ("queue" == flg) {
        val queue: String = jobInfo.get("queue")

        sql.append("INSERT INTO " + table + " (queue,jobid) VALUES ('%s','%s') ".format(queue, jobId))
        sql.append("ON DUPLICATE KEY UPDATE queue='%s' ".format(queue))

      }
      else if (("finish" == flg) || ("summary" == flg)) {
        val state: String = jobInfo.get("state")
        sql.append("UPDATE " + table + " SET state='%s',progress='%s',jobconf='%s' WHERE jobid='%s'".format(state, "FINISH", jobInfo.get("jobconf"), jobId))
      }
      else return ""

      sql.toString()
    }
    catch {
      case e: Exception => {
        e.printStackTrace
        System.err.println("###############################" + sql.toString())
      }
      return ""
    }

  }

  def openConnection(): Connection = {
    var conn: Connection = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, user, pass)
      if (conn.isClosed) throw new Exception("Mysql getConnection ERROR!")
    }
    catch {
      case e: Exception => {
        e.printStackTrace
      }
    }
    conn
  }

  def judgeFromJobStatus(id:String, clientCache: ClientCache, jobStatus: AppStatus, jobInfo: util.HashMap[String, String]): util.HashMap[String, String] = {
    val flg = jobInfo.get("flg")

    if ("submit" == flg) {
      var jobName: String = jobStatus.getJobName
      val mm: Matcher = pp.matcher(jobName)
      jobName = mm.replaceAll("")
      jobName = jobName.replaceAll("\"", "'")
      jobInfo.put("jobname", jobName)
      jobInfo.put("host", InetAddress.getByName(jobInfo.get("host")).getHostName + "/" + jobInfo.get("host"))
    }
    else if ("register" == flg) {
      val jobId = JobID.forName(id)
      jobInfo.put("jobconf", jobStatus.getJobFile)
      jobInfo.put("state", jobStatus.getState.toString)
      val mapNum = clientCache.getClient(jobId).getTaskReports(jobId, TaskType.MAP).length
      val redNum = clientCache.getClient(jobId).getTaskReports(jobId, TaskType.REDUCE).length
      jobInfo.put("mapnum", mapNum.toString)
      jobInfo.put("rednum", redNum.toString)
    }
    else if ("release" == flg) {
      if (jobStatus != null) {
        var mapProgress = jobStatus.getMapProgress
        var redProgress = jobStatus.getReduceProgress
        mapProgress = if (mapProgress.isNaN) 0.0f else mapProgress
        redProgress = if (redProgress.isNaN) 0.0f else redProgress
        jobInfo.put("mapprogress", mapProgress.toString)
        jobInfo.put("redprogress", redProgress.toString)
      }
    }
    else if (("finish" == flg) || ("summary" == flg)) {
      if (jobStatus == null)
        jobInfo.put("state", "FINISH")
      else {
        jobInfo.put("state", jobStatus.getState.toString)
        jobInfo.put("jobconf", jobStatus.getJobFile)
      }
    }
    jobInfo
  }

  def getJobStatus(jobs: Array[AppStatus], jobId: String): AppStatus = {
    jobs.find(job => job.getJobID == jobId).getOrElse(null)
  }

  def fetchMessageFromLog(str: String) = {
    var submit_matcher: Matcher = null
    var register_matcher: Matcher = null
    var queue_matcher: Matcher = null
    var release_matcher: Matcher = null
    var finish_matcher: Matcher = null
    var summary_matcher: Matcher = null

    var isMatch: Boolean = false
    val jobInfo: util.HashMap[String, String] = new util.HashMap[String, String]

    submit_matcher = submit_pattern.matcher(str)
    if (!isMatch && submit_matcher.matches) {
      jobInfo.put("time", submit_matcher.group(1))
      jobInfo.put("ugi", submit_matcher.group(2))
      jobInfo.put("host", submit_matcher.group(3))
      jobInfo.put("jobid", submit_matcher.group(5).replace("application", "job"))
      jobInfo.put("flg", "submit")
      isMatch = true
    }

    register_matcher = register_pattern.matcher(str)
    if (!isMatch && register_matcher.matches) {
      jobInfo.put("time", register_matcher.group(1))
      jobInfo.put("ugi", register_matcher.group(2))
      jobInfo.put("host", register_matcher.group(3))
      jobInfo.put("jobid", register_matcher.group(5).replace("application", "job"))
      jobInfo.put("flg", "register")
      isMatch = true
    }

    queue_matcher = queue_pattern.matcher(str)
    if (!isMatch && queue_matcher.matches) {
      jobInfo.put("time", queue_matcher.group(1))
      jobInfo.put("jobid", queue_matcher.group(2).replace("application", "job"))
      jobInfo.put("queue", queue_matcher.group(3))
      jobInfo.put("flg", "queue")
      isMatch = true
    }

    release_matcher = release_pattern.matcher(str)
    if (!isMatch && release_matcher.matches) {
      jobInfo.put("time", release_matcher.group(1))
      jobInfo.put("ugi", release_matcher.group(2))
      jobInfo.put("jobid", release_matcher.group(4).replace("application", "job"))
      jobInfo.put("flg", "release")
      isMatch = true
    }

    finish_matcher = finish_pattern.matcher(str)
    if (!isMatch && finish_matcher.matches) {
      jobInfo.put("time", finish_matcher.group(1))
      jobInfo.put("ugi", finish_matcher.group(2))
      jobInfo.put("jobid", finish_matcher.group(4).replace("application", "job"))
      jobInfo.put("flg", "finish")
      isMatch = true
    }

    summary_matcher = summary_pattern.matcher(str)
    if (!isMatch && summary_matcher.matches) {
      jobInfo.put("time", summary_matcher.group(1))
      jobInfo.put("jobid", summary_matcher.group(2).replace("application", "job"))
      jobInfo.put("flg", "summary")
      isMatch = true
    }

    if (isMatch) {
      (jobInfo.get("jobid"), jobInfo)
    } else {
      (null, null)
    }

  }

  def getAllJobs(client: ResourceMgrDelegate): Array[JobStatus] = {
    val appTypes = new util.HashSet[String](1)
    appTypes.add(MRJobConfig.MR_APPLICATION_TYPE)

    val appStates = new util.HashSet[YarnApplicationState]()
    appStates.add(YarnApplicationState.ACCEPTED)
    appStates.add(YarnApplicationState.FAILED)
    appStates.add(YarnApplicationState.KILLED)
    appStates.add(YarnApplicationState.NEW)
    appStates.add(YarnApplicationState.NEW_SAVING)
    appStates.add(YarnApplicationState.RUNNING)
    appStates.add(YarnApplicationState.SUBMITTED)

    val apps = client.getApplications(appTypes, java.util.EnumSet.copyOf(appStates))
    val jobs = TypeConverter.fromYarnApps(apps, getConf())
    //println("There are " + jobs.length + " jobs!")
    jobs
  }

  def getConf() = {
    val conf: Configuration = new Configuration()
    conf.set("mapreduce.framework.name", "yarn")
    conf.set("yarn.resourcemanager.address", rmAddress)
    conf.set("mapreduce.jobhistory.address", hisServerAddress)
    conf
  }

  def createYarnClient(): ResourceMgrDelegate = {
    try {
      val client = new ResourceMgrDelegate(new YarnConfiguration(getConf()))
      //println("connect to rm success!")
      client
    } catch {
      case e: Exception =>
        println("Exception happened when getting data from Resource Manager!")
        println(e.getMessage)
        null
    }
  }


  def readProperties(config: Properties) = {

    checkpointDirectory = config.getProperty("checkpointDirectory" ,"/data/xiaoju/spark/checkpoint")

    table = config.getProperty("db.table", "job_audit")
    driver = config.getProperty("db.driver", "com.mysql.jdbc.Driver")
    url=  config.getProperty("db.url", "jdbc:mysql://hdp801.qq:3306/monitor?autoReconnect=true&failOverReadOnly=false")
    user = config.getProperty("db.user", "root")
    pass = config.getProperty("db.password", "123456")

    topic = config.getProperty("topic","resourcemanager")
    brokerList = config.getProperty("brokers","hdp71.qq:9099,hdp72.qq:9092,hdp73.qq:9092")
    groupId = config.getProperty("group","spark-rmlog")

    rmAddress = config.getProperty("ResouceManager.Address","hdp800.qq:18040")
    hisServerAddress = config.getProperty("HistoryServer.Address","hdp800.qq:10020")
  }

  def main(args: Array[String]):Unit = {
/*    if(args.length > 0) {
      val in = this.getClass.getResourceAsStream(args(0))
      val config = new Properties()
      config.load(in)
      readProperties(config)
    }*/

    var ssc: StreamingContext = null
    try {
      ssc = createStreamingContext(args(0))
      ssc.start()
      ssc.awaitTermination()
    }catch {
      case e: Exception =>
        println(e.getMessage)
        ssc.stop(true)
    }


  }
}


