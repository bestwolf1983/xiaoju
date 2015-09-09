package com.xiaoju

import org.apache.hadoop.conf.Configuration

/**
 * Created by cenyuhai on 2015/9/9.
 */
class SparkHadoopConfiguration(conf: Configuration) extends Serializable {
  @transient var configuration: Configuration = conf
}
