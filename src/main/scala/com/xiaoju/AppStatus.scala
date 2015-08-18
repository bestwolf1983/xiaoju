package com.xiaoju

import org.apache.hadoop.mapreduce.{JobStatus, JobID}


/**
 * Created by Administrator on 15-4-16.
 */
class AppStatus(jobStatus: JobStatus) extends Serializable {
  var jobId: String = jobStatus.getJobID.toString
  var mapProgress: Float = jobStatus.getMapProgress
  var reduceProgress: Float = jobStatus.getReduceProgress
  var runState: String = jobStatus.getState.toString
  var jobName: String = jobStatus.getJobName
  var jobFile: String = jobStatus.getJobFile

  def getJobID = {
    jobId
  }

  def getJobName = {
    jobName
  }

  def getState = {
    runState
  }

  def getJobFile = {
    jobFile
  }

  def getMapProgress = {
    mapProgress
  }

  def getReduceProgress = {
    reduceProgress
  }
}
