package com.xiaoju

/**
 * Created by cenyuhai on 2015/9/24.
 */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HTable}

/**
 * Created by cenyuhai on 2015/6/17.
 */
class SHtable() extends Serializable {
  @transient val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "bigdata-arch-hdp277.bh:2181,bigdata-arch-hdp278.bh:2181,bigdata-arch-hdp279.bh:2181")

  @transient val table = new HTable(conf, "SF_ORDER2")

  def put(put: Put) = {
    table.put(put)
  }

  def close(): Unit = {
    table.close()
  }

}